#!/bin/sh
# 60-user-journey.sh — S1 GREEN: the "first-day user" journey on the real deploy stack (N=1 + 2 agents
# + ctl). Every assertion is cluster-semantics-free (N=1 per roadmap §0.4 topology minimization). The
# DEPLOY delta over the hermetic suites: real auth_callout JWT across the whole user-command surface +
# real CROSS-CONTAINER PTY byte-stream/signal/resize (via image/pty-run.py) + real User=sim isolation
# (hermetic runs all of this on an in-process embedded NATS). See docs/reviews/s1-plan.md §3.1.
#
# FALSE-GREEN GUARDS (this drill can go green for the wrong reason unless): (a) a cached session answers
# without a live CONNECT → G.3 asserts logout REFUSES + re-login reflects a mid-window state change;
# (b) a PTY assert accepts the 80x24 fallback → exact-size markers; (c) Ctrl-C "works" only because the
# sleep never started → a RUNNING pgrep baseline + a short post-Ctrl-C deadline; (d) an exit-code assert
# accepts "non-zero" → exact 7 / exact 128; (e) `stty size` matches echoed input → shell-computed markers.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab

# ── helpers (visible in assert_ok's command-substitution subshell) ───────────────────────────────────
CTL()    { "$SIM" ctl -- "$@"; }
# PTY driver: run pty-run.py inside ctl1 as user sim (session creds live under /home/sim/.tether), with
# an OUTER timeout watchdog so a wedged `tether run` can never hang the drill.
ptyrun() { "$SIM" exec ctl1 -- timeout "${PTY_TIMEOUT:-70}" runuser -u sim -- env HOME=/home/sim python3 /opt/sim/pty-run.py "$@"; }

_agt_online()   { "$SIM" ctl -- node ls 2>/dev/null    | grep -qE "^$1[[:space:]].*ONLINE"; }
_agt2_gone()    { "$SIM" ctl -- node ls -a 2>/dev/null  | grep -qE "^agt2[[:space:]]+(OFFLINE|STALE)"; }
_agt2_not_online_default() { ! "$SIM" ctl -- node ls 2>/dev/null | grep -qE "^agt2[[:space:]]"; }
# broker-authoritative observer (external review MAJOR-1): read brk1's node table DIRECTLY via its admin
# socket, as the broker's own uid (tether). This does NOT depend on a ctl active-session, so it is valid in
# the LOGOUT window and gives ground truth about the roster independent of any ctl client-side view.
_agt2_gone_admin() { "$SIM" exec brk1 -- runuser -u tether -- tether admin nodes 2>/dev/null | grep -qE "agt2[[:space:]]+(OFFLINE|STALE)"; }
# J14 (external review MAJOR-2): the PORTS section must render the EXACT N=1 header (6 cols, NO HOME column —
# ps.go renderPortsTable) plus an empty `(none)` body — NOT merely "the string PORTS appears somewhere" (a
# banner, a stray HOME column, or an unexpected allocated port would all pass the old loose grep).
_j14() {
    _j14_out=$("$SIM" ctl -- ps -a 2>/dev/null)
    printf '%s\n' "$_j14_out" | grep -qE '^[[:space:]]*NAME[[:space:]]+NODE[[:space:]]+LOCAL[[:space:]]+PUBLIC[[:space:]]+STATE[[:space:]]+CREATED[[:space:]]*$' \
    && printf '%s\n' "$_j14_out" | sed -n '/^PORTS$/,$p' | grep -qE '^[[:space:]]*\(none\)[[:space:]]*$'
}
# ps rows are `PID NODE STATE ... CMD`, so STATE (RUNNING/EXITED) comes BEFORE the CMD (sleep 5117).
_ps_running()   { "$SIM" ctl -- ps 2>/dev/null    | grep -E "sleep 5117" | grep -q RUNNING; }
_ps_exited()    { "$SIM" ctl -- ps -a 2>/dev/null | grep -E "sleep 5117" | grep -q EXITED; }
_sleep987_up()  { "$SIM" exec agt1 -- pgrep -f "sleep 987654" >/dev/null 2>&1; }
_sleep987_gone(){ ! "$SIM" exec agt1 -- pgrep -f "sleep 987654" >/dev/null 2>&1; }

# exec exit/stream oracles (result-not-exit-code where relevant)
_j4() { "$SIM" ctl -- exec agt1 -- sh -c 'exit 7' >/dev/null 2>&1; [ $? -eq 7 ]; }
_j5() {
    _j5_out=$("$SIM" ctl -- exec agt1 -- sh -c 'echo HEAD; head -c 262144 /dev/zero | tr "\0" x; echo; echo TAILxyz' 2>/dev/null)
    _j5_n=$(printf '%s' "$_j5_out" | wc -c)
    # S1-09: assert ORDER + contiguity — after stripping newlines, HEAD must precede the 256 KiB of x's
    # which precede TAILxyz (a tail-first or interleaved stream fails HEADx+TAILxyz), not just membership.
    printf '%s' "$_j5_out" | tr -d '\n\r' | grep -qE 'HEADx+TAILxyz' && [ "$_j5_n" -gt 262144 ]
}
_j6() { "$SIM" ctl -- exec --cwd /tmp agt1 -- pwd 2>/dev/null | grep -qx /tmp; }
_j7()  { "$SIM" ctl -- exec agt1 -- sh -c 'kill -TERM $$' >/dev/null 2>&1; [ $? -eq 128 ]; }
_j7b() { "$SIM" ctl -- exec agt1 -- sh -c 'kill -TERM $$' 2>&1 | grep -q 'terminated by signal'; }

drill_begin "60-user-journey (N=1 + 2 agents: login/G.3/node-ls/exec/run-PTY/ps/history)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 1 broker + 2 agents + 1 ctl"        "$SIM" up --brokers 1 --agents 2 --ctl 1
assert_ok "init brk1 (standalone → N=1 cluster)"  "$SIM" init brk1
assert_ok "N=1 leader floor (leader=brk1)"        sh -c "\"$SIM\" exec brk1 -- sh -c 'tether cluster status --json' | jq -e '.leader_id==\"brk1\" and (.health==\"DEGRADED\" or .health==\"HEALTHY_HA\")' >/dev/null"
assert_ok "session $SID + ctl login"              "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"                       "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_ok "agent-join agt2"                       "$SIM" agent-join agt2 --session "$SID" --pin "$PIN"
assert_ok "both agents ONLINE (2-agent baseline)" sh -c "\"$SIM\" ctl -- node ls | grep -qE '^agt1[[:space:]].*ONLINE' && \"$SIM\" ctl -- node ls | grep -qE '^agt2[[:space:]].*ONLINE'"

# ── J1: ctx shows the active session ─────────────────────────────────────────────────────────────────
assert_ok "J1 ctx shows the active session ($SID)"  sh -c "\"$SIM\" ctl -- ctx | grep -qw $SID"

# ── J-G.3: logout REFUSES → PROVE the roster changed DURING the logout window (broker-authoritative) →
# re-login → the FIRST post-login read already reflects it. HONEST FRAMING (external review MAJOR-1):
# tether's `login` has NO snapshot semantics — it does ONE auth_callout CONNECT then closes + writes
# current_session (cmd/tether/login.go); each later `node ls` is a fresh CONNECT + node.list.req. So the
# deploy claim is NOT "login fetches a snapshot"; it is "after a real disconnect the RE-AUTHENTICATED
# session's FIRST read returns CURRENT broker state, reflecting a change that demonstrably happened while
# disconnected." A poll_until AFTER login would false-green on natural post-login heartbeat expiry, so the
# causality is pinned by (b2) an authoritative observer with NO ctl session + (c-2) the FIRST read as oracle.
assert_ok       "J-G.3a-1 logout clears current_session"                          "$SIM" ctl -- logout
assert_refuses  "J-G.3a-2 node ls REFUSES with no active session (negative control)" \
                "no active session" \
                "$SIM" ctl -- node ls
# inject a roster change WHILE the ctl is logged out (no ctl dependency): stop agt2's daemon.
assert_ok       "J-G.3b stop agt2 daemon (state change during the logout window)"  "$SIM" exec agt2 -- systemctl stop tether-agent
# PROVE the change reached the broker's table WHILE the ctl is logged out, via brk1's admin socket (needs
# NO ctl session). This establishes agt2 went STALE/OFFLINE BEFORE re-login, so the post-login oracle below
# cannot be satisfied by expiry that only happens after login — it isolates the re-auth-then-read causality.
assert_ok       "J-G.3b2 broker admin view shows agt2 STALE/OFFLINE DURING the logout window (authoritative, no ctl session)" \
                poll_until 30 2 "agt2 STALE/OFFLINE in brk1 admin nodes" -- _agt2_gone_admin
assert_ok       "J-G.3c-1 re-login (fresh CONNECT + auth_callout re-auth)"          "$SIM" ctl -- login -s "$SID" --pin "$PIN"
# THE oracle: the FIRST node-list after re-login (a SINGLE check, NOT a poll) must ALREADY show agt2 gone.
# Since J-G.3b2 proved the state changed before login, GREEN here proves the reconnected session's first
# read returns CURRENT state; a client that served a cached pre-logout roster would show agt2 ONLINE → RED.
assert_ok       "J-G.3c-2 FIRST post-login node ls -a already reflects agt2 gone (reconnected read = current state; single check, no poll)" \
                _agt2_gone
assert_ok       "J-G.3c-3 default node ls (ONLINE-only) drops agt2, keeps agt1"    sh -c "\"$SIM\" ctl -- node ls | grep -qE '^agt1[[:space:]].*ONLINE'"
assert_ok       "J-G.3c-4 default node ls no longer lists agt2"                    _agt2_not_online_default
# restore agt2 ONLINE for the rest of the journey (not strictly needed; keeps the roster clean).
"$SIM" exec agt2 -- systemctl start tether-agent >/dev/null 2>&1 || true

# ── J2: node ls real columns (ONLINE + int PROTO + non-empty RELEASE) ────────────────────────────────
# KIND (device|leased) was inserted between NODE and STATUS by 1e9d32a (2026-08-19) and this row —
# whose whole job is to pin the REAL column layout — was never updated, so it had been unable to
# match since. The fix ADDS the column rather than relaxing the pattern: a shape assertion that
# tolerates the shape changing is not a shape assertion.
assert_ok "J2 node ls columns: agt1 KIND, ONLINE, PROTO int, RELEASE non-empty" \
    sh -c "\"$SIM\" ctl -- node ls | grep -qE '^agt1[[:space:]]+(device|leased)[[:space:]]+ONLINE[[:space:]]+[^[:space:]]+[[:space:]]+[0-9]+[[:space:]]+[^[:space:]]'"

# ── J3-J7: exec (exit 0 / exact non-zero / stream / --cwd / signal→128) ──────────────────────────────
assert_ok "J3 exec exit-0 passthrough"                                 "$SIM" ctl -- exec agt1 -- true
assert_ok "J4 exec non-zero exit passthrough (EXACT 7, not just !=0)"   _j4
assert_ok "J5 exec stdout multi-chunk stream (256 KiB, ordered HEAD..TAILxyz)"  _j5
assert_ok "J6 exec --cwd /tmp (flag BEFORE node; SetInterspersed)"     _j6
assert_ok "J7 exec signal-kill → FLAT 128 (not 128+signo)"             _j7
assert_ok "J7b exec signal-kill stderr note 'terminated by signal'"    _j7b

# ── J8-J12: run over a REAL cross-container PTY (via pty-run.py) ──────────────────────────────────────
# J8: the far-side `stty size` reflects the pty window we set (40x132) — not the 80x24 fallback. Uses the
# NON-INTERACTIVE `sh -c` form (self-outputs on attach, no stdin race). Interactive steps (J9-J11) instead
# expect the shell prompt `$ ` FIRST, so input is never sent before the remote shell is reading it.
assert_ok "J8 run real PTY size 40x132 (cross-container winsize)" \
    ptyrun --rows 40 --cols 132 --idle-timeout 30 \
        --step 'expect:40 132' \
        -- tether run agt1 -- sh -c 'stty size'
# J9: interactive round-trip with a SHELL-COMPUTED marker (input has R%sK, output is ROK — no echo match).
assert_ok "J9 run interactive round-trip (shell-computed marker ROK)" \
    ptyrun --rows 40 --cols 132 --idle-timeout 30 \
        --step 'expect:$ ' --step "send:printf 'R%sK\n' O" --step 'expect:ROK' --step 'send:exit' \
        -- tether run agt1 -- sh
# J10: SIGWINCH resize propagates to the remote master (distinct 2nd size catches a stuck first size).
assert_ok "J10 run resize propagates (24x80 → remote stty size)" \
    ptyrun --rows 40 --cols 132 --idle-timeout 30 \
        --step 'expect:$ ' \
        --step 'send:stty size' --step 'expect:40 132' \
        --step 'resize:24x80' --step 'send:stty size' --step 'expect:24 80' \
        --step 'send:exit' \
        -- tether run agt1 -- sh
# J11: Ctrl-C (0x03) → line discipline → SIGINT to the remote process group. Baseline: the foreground
# `sleep 987654` is RUNNING on agt1 before the interrupt (guards "sleep never started"). Oracle A: the
# shell becomes responsive again within a SHORT post-Ctrl-C deadline (a still-blocked shell fails fast).
( ptyrun --idle-timeout 30 \
        --step 'expect:$ ' \
        --step 'send:sleep 987654' --step 'sleep:5' \
        --step ctrlc \
        --step "send:printf 'B%sK\n' AC" --step 'expect:BACK:8' \
        --step 'send:exit' \
        -- tether run agt1 -- sh ) >/tmp/sim-60-j11.out 2>&1 &
_J11=$!
assert_ok "J11-base foreground sleep RUNNING on agt1 (pre-injection baseline)" \
    poll_until 30 1 "sleep 987654 running on agt1" -- _sleep987_up
wait "$_J11"; _J11RC=$?
assert_ok "J11 Ctrl-C interrupted the foreground sleep (prompt returned ≤8s)" \
    sh -c "[ $_J11RC -eq 0 ]"
# Oracle B: the sleep process group is gone (poll for async teardown).
assert_ok "J11b sleep pgroup gone after Ctrl-C (poll async teardown)" \
    poll_until 15 1 "sleep 987654 gone on agt1" -- _sleep987_gone

# ── J13-J14: ps state machine + PORTS section ────────────────────────────────────────────────────────
# exec IS a managed proc in ps (STATE=RUNNING while live). Unique CMD `sleep 5117` avoids matching others.
"$SIM" ctl -- exec agt1 -- sleep 5117 >/dev/null 2>&1 &
_J13=$!
assert_ok "J13a ps shows the sleep RUNNING (default view)" \
    poll_until 12 1 "sleep 5117 RUNNING in ps" -- _ps_running
"$SIM" exec agt1 -- pkill -f "sleep 5117" >/dev/null 2>&1 || true
wait "$_J13" 2>/dev/null || true
assert_ok "J13b ps -a shows the sleep EXITED (transition; -a gates it)" \
    poll_until 15 1 "sleep 5117 EXITED in ps -a" -- _ps_exited
# N=1 → NO HOME column (HOME is cluster-only). Assert the EXACT 6-col PORTS header + empty (none) body —
# not just that the string PORTS appears somewhere (external review MAJOR-2).
assert_ok "J14 ps PORTS section: exact 6-col header (no HOME) + empty (none) body" _j14

# ── J15-J17: history (-n bound / --kind + invalid reject / --follow smoke) ────────────────────────────
# history rows are `HH:MM:SS  KIND  ...` (timestamp FIRST, kind SECOND) — anchoring on ^KIND is a false-green.
_HKIND='[0-9]{2}:[0-9]{2}:[0-9]{2}[[:space:]]+'
assert_ok "J15 history -n 5 bounds output (1..5 audit rows)" \
    sh -c "n=\$(\"$SIM\" ctl -- history -n 5 2>/dev/null | grep -cE '${_HKIND}(CALL|PROC|PORT|XFER)'); [ \"\$n\" -ge 1 ] && [ \"\$n\" -le 5 ]"
assert_ok "J16a history --kind proc lists ONLY PROC rows (real JS audit replay)" \
    sh -c "\"$SIM\" ctl -- history --kind proc 2>/dev/null | grep -qE '${_HKIND}PROC' && ! \"$SIM\" ctl -- history --kind proc 2>/dev/null | grep -qE '${_HKIND}(CALL|PORT|XFER)'"
assert_refuses "J16b history --kind bogus rejected (validator: must be one of)" \
    "must be one of" \
    "$SIM" ctl -- history --kind bogus
assert_ok "J17 history --follow smoke (bounded; a new record tails in)" \
    sh -c "( \"$SIM\" ctl -- history --follow >/tmp/sim-60-follow.out 2>&1 & fpid=\$!; sleep 1; \"$SIM\" ctl -- exec agt1 -- true >/dev/null 2>&1; \"$SIM\" ctl -- exec agt1 -- true >/dev/null 2>&1; sleep 3; kill \$fpid 2>/dev/null; grep -qE '${_HKIND}(CALL|PROC|PORT|XFER)' /tmp/sim-60-follow.out )"

# ── J18: version / completion (pure-local smoke, not a deploy-value claim) ────────────────────────────
assert_ok "J18a version prints a semver"        sh -c "\"$SIM\" ctl -- version | grep -qE '[0-9]+\.[0-9]+\.[0-9]+'"
assert_ok "J18b completion bash renders"        sh -c "\"$SIM\" ctl -- completion bash | grep -qE 'complete|_tether'"

drill_end
