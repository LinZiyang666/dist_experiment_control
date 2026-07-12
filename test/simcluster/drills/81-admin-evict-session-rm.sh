#!/bin/sh
# 81-admin-evict-session-rm.sh — S2 GREEN (N=1): the broker admin socket + agent eviction + session rm
# three-phase, on the real deploy stack. Deploy delta over hermetic: real 0600/0700 admin unix socket OS
# EACCES (no app string) + real sys.events broadcast → real cross-process agent self-exit + real FK cascade
# on real SQLite + real JS history-stream delete + a REAL orphan OS process (the in-process suites cannot
# manufacture one). Consumes S0-隧道 for the active-expose arm. See docs/reviews/s2-plan.md §3.2.
#
# FALSE-GREEN GUARDS:
#  (a) admin-owner arm: `nobody`'s "permission denied" could be a missing binary / bad --socket / broker
#      down, not the 0600-socket EACCES → sig pins `connect: permission denied` + a positive control (same
#      binary + socket as user tether → OK).
#  (b) evict self-exit: "agent left roster" could be a heartbeat expiry, not the evict broadcast → the
#      oracle is `pgrep -x tether` on agt1 EMPTY (the daemon PROCESS exited) within the ~1s P9 budget.
#  (c) reconnect-denied: post-evict reconnect could fail "broker unreachable" not "provisioning wall" →
#      client sig pins `NATS auth_callout rejected` (PRINTED agent.go:859, gated by isAuthFailure agent.go:1538
#      on Authorization Violation ONLY; an unreachable broker → silent retry, no such string). That message is
#      itself auth-SPECIFIC, so B3 needs NO separate server-side discriminator (R21). NEVER assert the
#      server-only reason `not provisioned…` on the client (§3.0).
#  (d) evict-cleanup GAP (探索→定格, INVERTED): a leaked child = pgrep HIT = exit 0 → assert_ok (assert_bug
#      would misread the leak as "APPEARS FIXED"). Composite oracle: daemon EXITED **and** child STILL ALIVE.
#      Deployment-conditional (spike#3): the leak is only visible under a setsid-nohup deploy (systemd cgroup
#      reaps the child otherwise — that reap is systemd's doing, NOT tether's).
#  (e) session-rm: "the rm journey ran" is NOT phase coverage → each phase has its own RESULT oracle
#      (history stream gone / SQLite rows gone), polled — never the rm exit code.
#  (f) session_deleting probe: recording covered because rm ran is forbidden (inventory §1.2). After N=1
#      SYNCHRONOUS rm the session ROW is GONE, so a fresh session-scoped call is refused at CONNECT by
#      auth_callout (ensureMember→session-not-active, BROKER-side, no agent broadcast — DOC-11's core claim).
#      The app-layer `session.IsActive` gate emitting `session_not_found_or_deleting` (exec.go:49-55) needs a
#      session that still EXISTS but is DELETING — unreachable in N=1 sync rm → hermetic-only, NOT deploy-
#      covered here. The probe pins the CONNECT-deny path + logs the promise-vs-impl gap (DOC-11).
#  (g) re-join arm (INVERTED): re-join with the same session PIN SUCCEEDS → the guard is the SAME evicted
#      nkey (fp-equality oracle), not a fresh identity.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/agentyaml.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
NURL="nats://brk1:4222"
EVCAP=/tmp/sim-81-ev.cap

CTL()    { "$SIM" ctl -- "$@"; }
BADMIN() { "$SIM" exec brk1 -- runuser -u tether -- tether admin "$@"; }
AGT()    { "$SIM" exec "$1" -- "$@"; }   # unused-friendly; call as: "$SIM" exec agt1 -- <cmd>

# broker-authoritative predicates. NB `tether admin nodes` prints its table to STDOUT (verified live via
# fd separation — fd2 is empty; tether is correct here). Its format is `SESSION NODE STATE …` — SESSION is
# the FIRST column, so a `^agt1` anchor NEVER matches (the line starts with the sid). Match agt1 + ONLINE
# without a leading anchor. (2>&1 is immaterial since stderr is empty; kept only for tidiness.)
_agt1_online()  { BADMIN nodes 2>&1 | grep -E "agt1[[:space:]]" | grep -q ONLINE; }
_agt1_gone()    { ! BADMIN nodes 2>&1 | grep -qE "agt1[[:space:]]"; }
_daemon_gone()  { ! "$SIM" exec agt1 -- pgrep -x tether >/dev/null 2>&1; }
_child_alive()  { "$SIM" exec agt1 -- pgrep -f 'sleep 999999' >/dev/null 2>&1; }
_child2_alive() { "$SIM" exec agt1 -- pgrep -f 'sleep 888888' >/dev/null 2>&1; }
# leak oracle: daemon gone AND managed child still alive (the divergence)
_leak_present() { _daemon_gone && _child_alive; }
# session rm phase oracles
_history_gone() { ! BADMIN audit "$SID" -n 5 >/dev/null 2>&1; }
_sess_gone()    { ! BADMIN sessions 2>/dev/null | grep -qw "$SID"; }
# sys.events observer (owner ctl is a lab member; its 24h JWT survives session destroy)
ev_sub_start()  { "$SIM" exec ctl1 -- sh -c "nohup runuser -u sim -- env HOME=/home/sim nats --server $NURL --no-context --nkey /home/sim/.tether/keys/default.nk --connection-name tether-cli:$SID sub 'tether.v2.sys.events' >$EVCAP 2>&1 & echo ok" >/dev/null 2>&1; }
ev_ready()      { "$SIM" exec ctl1 -- grep -q 'Subscribing' "$EVCAP" 2>/dev/null; }
ev_stop()       { "$SIM" exec ctl1 -- pkill -f 'sys.events' >/dev/null 2>&1 || true; }
_ev_destroyed() { "$SIM" exec ctl1 -- sh -c "grep '\"type\":\"session_destroyed\"' $EVCAP | grep -q '\"sid\":\"$SID\"'"; }

# capture agt1's provisioning fingerprint from the broker (admin nodes --json), for the fp-equality oracle

drill_begin "81-admin-evict-session-rm (N=1: admin socket + evict + session rm 3-phase)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 1 broker + 1 agent + 1 ctl"          "$SIM" up --brokers 1 --agents 1 --ctl 1
assert_ok "init brk1 (standalone → N=1 cluster)"   "$SIM" init brk1
assert_ok "N=1 leader floor (leader=brk1)"         sh -c "\"$SIM\" exec brk1 -- sh -c 'tether cluster status --json' | jq -e '.leader_id==\"brk1\"' >/dev/null"
assert_ok "session lab + ctl login (owner)"        "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1 (bind nkey)"            "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_ok "provision agt1 agent.yaml (tunnel_addr wired, S0-隧道)" \
                                                   agent_provision_yaml agt1 "$SID" "$NURL" open

# ── Arm A — admin socket owner semantics ─────────────────────────────────────────────────────────────
assert_ok      "A1 admin sessions (authorized) lists lab ACTIVE" \
               sh -c "\"$SIM\" exec brk1 -- runuser -u tether -- tether admin sessions | grep -qw $SID"
assert_ok      "A2 admin nodes (authorized) shows agt1 ONLINE"  _agt1_online
assert_ok      "A3 admin audit (authorized, history-lab EXISTS)" \
               sh -c "\"$SIM\" exec brk1 -- runuser -u tether -- tether admin audit $SID -n 5 >/dev/null"
assert_refuses "A4 non-authorized OS user (nobody) → OS EACCES connect: permission denied" \
               "connect: permission denied" \
               "$SIM" exec brk1 -- runuser -u nobody -- tether admin sessions

# ── Arm B — evict online agent → ~1s self-exit + reconnect refused ───────────────────────────────────
assert_ok      "B0 baseline: agt1 ONLINE (pre-injection)"       _agt1_online
assert_ok      "B1 admin evict (2-arg) deletes + broadcasts" \
               sh -c "\"$SIM\" exec brk1 -- runuser -u tether -- tether admin evict $SID agt1 | grep -qE 'evicted sid=lab nid=agt1.*broadcast=true'"
assert_ok      "B2a agent daemon self-exits ≤~1s (PROCESS gone, not heartbeat expiry)" \
               poll_until 10 1 "agt1 tether daemon exited" -- _daemon_gone
assert_ok      "B2b roster row deleted (FK)" \
               poll_until 10 1 "agt1 gone from broker roster" -- _agt1_gone
# B3 client string is AUTH-SPECIFIC (unlike the ctl §3.0 case): agent.go:1538-1553 prints "NATS auth_callout
# rejected" ONLY on an Authorization Violation; an unreachable broker → silent retry (no such message). So
# B3 alone distinguishes an auth-deny from a network fault — no separate server-side discriminator needed.
assert_refuses "B3 reconnect refused (provisioning gone) — agent-specific auth-callout rejection (distinguishes auth-deny from unreachable)" \
               "auth_callout rejected|Authorization Violation" \
               "$SIM" exec agt1 -- runuser -u sim -- env HOME=/home/sim timeout 8 tether agent --session "$SID" --nid agt1

# ── Arm D — evicted nkey re-join (探索→定格, INVERTED; DOC-6; re-provisions agt1 for Arm C) ────────────
# D0: capture the evicted agent's ON-DISK identity nkey (byte) hash. It PERSISTS across evict — evict
# deletes the BROKER-side agent_provisioning row (§ #26), NOT the agent's local key file
# (<home>/agent/<sid>/keys/agent.nk; EnsureAgentIdentity LOADS if present, identity.go:57). D1 then proves
# the SAME byte-identical nkey re-provisions (not a fresh mint) → eviction is NOT identity revocation (DOC-6).
NKPATH="/home/sim/.tether/agent/$SID/keys/agent.nk"
NKHASH0=$("$SIM" exec agt1 -- sh -c "md5sum $NKPATH 2>/dev/null | awk '{print \$1}'")
log "81: evicted agent nkey (byte) hash: ${NKHASH0:-<none captured>}"
_d1_rejoin_same_nkey() {
    [ -n "$NKHASH0" ] || return 1                                    # guard: no baseline → not vacuously true
    "$SIM" agent-join agt1 --session "$SID" --pin "$PIN" >/dev/null 2>&1 || return 1
    _agt1_online || return 1
    _h=$("$SIM" exec agt1 -- sh -c "md5sum $NKPATH 2>/dev/null | awk '{print \$1}'")
    [ "$_h" = "$NKHASH0" ]                                           # SAME nkey byte-identical (not re-minted)
}
assert_ok      "D1 evicted nkey (byte-identical, md5==D0) re-provisions with same session PIN → ONLINE (eviction ≠ revocation, no denylist; DOC-6)" \
               _d1_rejoin_same_nkey
assert_ok      "D1-reprovision agt1 agent.yaml (tunnel_addr, for Arm C)" \
               agent_provision_yaml agt1 "$SID" "$NURL" open

# ── Arm C — evict-with-RUNNING-proc + ACTIVE-EXPOSE cleanup GAP (setsid-nohup deploy; spike#3 forms) ──
# redeploy agt1 via setsid-nohup (NOT systemd cgroup) so daemon + managed child are in separate process trees
assert_ok "C-setup stop systemd unit"              "$SIM" exec agt1 -- systemctl stop tether-agent
"$SIM" exec agt1 -- sh -c "cd /home/sim && setsid nohup runuser -u sim -- env HOME=/home/sim tether agent --session $SID --nid agt1 --nats-url $NURL --tunnel-addr brk1:7000 >/tmp/agtC.log 2>&1 & echo ok" >/dev/null 2>&1
assert_ok "C-setup agt1 ONLINE again (setsid-nohup deploy)" \
          poll_until 30 2 "agt1 ONLINE (setsid-nohup)" -- _agt1_online
# managed child: a backgrounded exec (registers in the processes table — spike#3)
"$SIM" exec ctl1 -- env HOME=/home/sim sh -c "setsid runuser -u sim -- env HOME=/home/sim tether exec agt1 -- sleep 999999 >/tmp/execchild.log 2>&1 & echo ok" >/dev/null 2>&1
assert_ok "C-base-proc managed child RUNNING (pre-injection baseline, agent-host pgrep)" \
          poll_until 15 1 "sleep 999999 running on agt1" -- _child_alive
# R6: also establish the BROKER-DB baseline (a managed `processes` row RUNNING) so C-brk's post-evict
# absence check genuinely proves an FK cascade removed a row that DID exist — not a vacuous pass on a row
# that was never inserted (the DIVERGENCE half of #26).
assert_ok "C-base-proc-db broker `ps` shows the child RUNNING (pre-injection DB baseline for the divergence)" \
          poll_until 15 1 "sleep 999999 RUNNING in broker ps" -- sh -c "\"$SIM\" ctl -- ps -a 2>/dev/null | grep 'sleep 999999' | grep -q RUNNING"
# active expose serving a SENTINEL body (curl-able cross-container — the data-plane baseline)
TOKEN="SENTINEL81_$$"
"$SIM" exec agt1 -- sh -c "printf '%s' '$TOKEN' > /home/sim/probe.txt; chown sim:sim /home/sim/probe.txt"
"$SIM" exec agt1 -- sh -c "cd /home/sim && (setsid runuser -u sim -- python3 -m http.server 8080 --bind 127.0.0.1 >/tmp/httpd.log 2>&1 &)" >/dev/null 2>&1
# R19: poll for the local server to bind (no fixed sleep).
poll_until 6 1 "http.server 8080 bound on agt1" -- sh -c "\"$SIM\" exec agt1 -- curl -sf --max-time 2 http://127.0.0.1:8080/probe.txt >/dev/null 2>&1" || true
# R11: parse the public port from the MACHINE surface (--json | jq .port), NOT a grep of the 2>&1-merged
# human line (where a stray :4222 from a transport log would win head -1 → wrong port → vacuous C-port).
PORT=$("$SIM" ctl -- expose agt1 --local 8080 --name web --json 2>/dev/null | jq -r '.port'); echo "  expose port=$PORT"
_curl_probe() { "$SIM" exec ctl1 -- sh -c "curl -sf --max-time 5 http://brk1:$PORT/probe.txt" 2>/dev/null | grep -q "$TOKEN"; }
# F3: the port is CLEANED only if the TRANSPORT is connection-REFUSED (curl exit 7), NOT merely `! curl -sf`
# (which is also nonzero for a still-LISTENING leaked port returning 4xx/5xx). Plus the broker's authoritative
# port-allocation row (expose name 'web' in the ps PORTS section) must be GONE (FK cascade).
_c_port_refused()  { "$SIM" exec ctl1 -- sh -c "curl -s -o /dev/null --max-time 5 http://brk1:$PORT/probe.txt; [ \$? -eq 7 ]"; }
_c_port_row_gone() { ! "$SIM" ctl -- ps -a 2>/dev/null | sed -n '/^PORTS$/,$p' | grep -qw web; }
_c_port_clean()    { _c_port_refused && _c_port_row_gone; }
assert_ok "C-base-expose active expose curl-able (SENTINEL body cross-container; pre-injection baseline)" \
          poll_until 15 1 "expose serves SENTINEL" -- _curl_probe
# INJECT: evict at {agt1 ONLINE, child RUNNING, expose live}
assert_ok "C1 admin evict at active state" \
          sh -c "\"$SIM\" exec brk1 -- runuser -u tether -- tether admin evict $SID agt1 | grep -qE 'evicted sid=lab nid=agt1.*broadcast=true'"
# OBSERVE the divergence
assert_ok "C-brk broker DB proc+node rows GONE (FK cascade — evict contract)" \
          poll_until 15 1 "sleep 999999 row + agt1 gone from broker DB" -- sh -c "! \"$SIM\" ctl -- ps -a 2>/dev/null | grep -q 'sleep 999999' && ! \"$SIM\" exec brk1 -- runuser -u tether -- tether admin nodes 2>&1 | grep -qE 'agt1[[:space:]]'"
assert_ok "C-exit agent daemon exited" \
          poll_until 10 1 "agt1 daemon gone" -- _daemon_gone
assert_ok "C-GAP-proc [#26] LEAKED OS child — daemon EXITED but managed child STILL ALIVE (setsid-nohup; INVERTED assert_ok = the gap)" \
          _leak_present
assert_ok "C-port public expose port CLEANED: transport CONNECTION-REFUSED (curl exit 7, NOT a 4xx/5xx leaked listener; F3) + broker port-alloc row gone (FK cascade); mechanism = tunnel drop on daemon exit" \
          poll_until 15 1 "expose port connection-refused + alloc row gone" -- _c_port_clean
# reap the leaked child before the systemd counter-factual
"$SIM" exec agt1 -- pkill -f 'sleep 999999' >/dev/null 2>&1 || true

# ── C-GAP-proc-sysd — counter-factual: under systemd, cgroup reaps the child (NOT tether's doing) ─────
assert_ok "C-sysd-setup re-join agt1 under SYSTEMD unit" \
          sh -c "\"$SIM\" agent-join agt1 --session $SID --pin $PIN >/dev/null 2>&1 && \"$SIM\" exec brk1 -- runuser -u tether -- tether admin nodes 2>&1 | grep -E 'agt1[[:space:]]' | grep -q ONLINE"
"$SIM" exec ctl1 -- env HOME=/home/sim sh -c "setsid runuser -u sim -- env HOME=/home/sim tether exec agt1 -- sleep 888888 >/dev/null 2>&1 & echo ok" >/dev/null 2>&1
assert_ok "C-sysd-base systemd-deployed managed child RUNNING" \
          poll_until 15 1 "sleep 888888 running" -- _child2_alive
assert_ok "C-sysd-evict evict the systemd-deployed agent" \
          sh -c "\"$SIM\" exec brk1 -- runuser -u tether -- tether admin evict $SID agt1 | grep -qE 'evicted sid=lab nid=agt1.*broadcast=true'"
assert_ok "C-GAP-proc-sysd counter-factual: systemd cgroup REAPS the child (NOT tether — proves #26 is deployment-conditional)" \
          poll_until 12 1 "sleep 888888 reaped by cgroup" -- sh -c "! \"$SIM\" exec agt1 -- pgrep -f 'sleep 888888' >/dev/null 2>&1"

# ── Arm E — session rm 3-phase + session_deleting probe (agt1 already evicted; session-level) ─────────
ev_sub_start
assert_ok      "E-sub2 sys.events observer live (owner is a lab member; 24h JWT survives destroy)" \
               poll_until 15 1 "sys.events Subscribing" -- ev_ready
assert_ok      "E-base baseline: lab ACTIVE + history-lab exists (pre-deletion)" \
               sh -c "\"$SIM\" ctl -- session ls | grep -qw $SID && \"$SIM\" exec brk1 -- runuser -u tether -- tether admin sessions | grep -qw $SID"
assert_ok      "E1 session rm (owner)"             "$SIM" ctl -- session rm "$SID"
assert_ok      "E2a phase②: history-lab stream deleted (RESULT)" \
               poll_until 15 1 "history-lab unavailable" -- _history_gone
assert_ok      "E2b phase③: SQLite session row gone (RESULT)" \
               poll_until 15 1 "lab gone from broker sessions" -- _sess_gone
assert_ok      "E2c phase④: session_destroyed sys.event captured (direct oracle)" \
               poll_until 15 1 "session_destroyed for lab" -- _ev_destroyed
ev_stop
# DOC-11 probe: after rm COMPLETES the session row is GONE, so a fresh session-scoped call is refused at
# CONNECT by auth_callout (the BROKER enforces it — there is NO agent-side session_deleting broadcast, which
# usage §5.10 wrongly promises; the raw broker code session_not_found_or_deleting is server-side only, §3.0,
# and the app-layer IsActive gate is only reachable for a session that still EXISTS but is DELETING — a
# window that N=1 synchronous rm closes before returning, hence hermetic-only for the code taxonomy).
assert_refuses "E3a session-scoped exec on the removed sid → refused broker-side at CONNECT (no agent broadcast; DOC-11)" \
               "auth_callout rejected|Authorization Violation" \
               "$SIM" ctl -- exec agt1 -- true
assert_refuses "E3b same refusal on expose (independent CLI path → same broker-enforced refusal)" \
               "auth_callout rejected|Authorization Violation" \
               "$SIM" ctl -- expose agt1 --local 8080 --name x
# R17 (main-process CORRECTED the review): `session rm` connects as ACTIVATED tether-cli:<sid>
# (cmd/tether/session.go:163 `CtlNameForSession(args[0])`), so a 2nd rm on the now-absent lab is denied at
# CONNECT too — it never reaches the rm handler, so the handler's `not_found` (sessions.go:168) is NOT
# deploy-reachable post-rm-complete. The rm-API taxonomy (not_found / already_deleting) needs the session to
# still EXIST at CONNECT time → hermetic-only (test/p3/session_e2e_test). This arm pins the deploy reality:
# EVERY post-rm session-scoped op — including a 2nd rm — uniformly hits the broker CONNECT-deny.
assert_refuses "E3c 2nd session rm on the absent sid → also CONNECT-denied (rm-API not_found is hermetic-only, unreachable post-rm)" \
               "auth_callout rejected|Authorization Violation" \
               "$SIM" ctl -- session rm "$SID"

ev_stop
drill_end
