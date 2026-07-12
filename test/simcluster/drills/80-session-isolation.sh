#!/bin/sh
# 80-session-isolation.sh — S2 GREEN: multi-tenant session isolation on the REAL auth_callout stack
# (N=1 + 2 agents + ctl). Only a real out-of-process nats-server ACL denial is a REAL denial. Deploy delta
# over hermetic: real auth_callout JWT → PROTOCOL-LAYER ACL denial ("Permissions Violation", verified live
# spike#4) + real sys.events (pin_failed/member_joined) + real per-session node table + distinct-nkey dual
# identity via distinct $TETHER_HOME. See docs/reviews/s2-plan.md §3.1.
#
# FALSE-GREEN GUARDS (this drill can go green for the wrong reason unless):
#  (a) a CONNECT-deny arm's client string is the GENERIC `Authorization Violation` (a bad broker / network
#      fault denies too) → every deny is paired with a POSITIVE control (same identity activates its OWN
#      session, exit 0) + a server-side discriminator (wrong-PIN → pin_failed sys.event; non-member → ctx
#      NOT persisted). auth_callout reasons are SERVER-ONLY (§3.0; handler.go:400-405 → nats-server generic
#      `Authorization Violation` client.go:2434) — NEVER assert the reason string on the client.
#  (b) bare-NATS cross-session deny is a DISCONNECT (identity never got a JWT) not a subject-scoped ACL →
#      a pub POSITIVE control (A→s.lab.* pub exit 0) proves the JWT is valid; the deny sig pins
#      `Permissions Violation for (Publish|Subscription) to.*s.<other-sid>` (connect-deny says `Authorization
#      Violation`, NEVER `Permissions Violation`).
#  (c) `not_owner` is a CONNECT-deny in disguise → not_owner is an APP-LAYER code only reachable AFTER a
#      successful member CONNECT (a non-member hits `Authorization Violation` first) + a member positive control.
#  (d) the PIN rate-limit probe: the 11th same-IP correct-PIN join SUCCEEDING is the RECORDED DEFECT
#      (assert_ok, INVERTED — §E.6 unimplemented, gotcha #25). If a maintainer sees it FAIL (11th refused) a
#      limiter landed → FLIP to assert_refuses.
#  (e) a sys.events oracle catches a STALE setup event → the background core sub starts AFTER setup; only
#      W's post-sub join is asserted; the capture file is fresh; Arm R runs AFTER the sub is killed (E-clean).
#  (f) `member_joined`: the owner's own `login` does NOT emit it (already a member → fp-reconnect path,
#      handler.go:320-330) → the member_joined assertion uses a FRESH non-member (W), never owner/setup login.
#  (g) `TETHER_SESSION` no-crosstalk: `node ls` might ignore $TETHER_SESSION → agt1 only in lab, agt2 only in
#      ops; each shell shows only its own session's node (bidirectional cross-check).
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/ident.sh"
SIM="${SIM:-$HERE/simcluster}"

SID_A=lab;  PIN_A=135790
SID_B=ops;  PIN_B=246810
NURL="nats://brk1:4222"
EVCAP=/tmp/sim-80-ev.cap

# ── event-capture helpers (core sub on the global sys.events subject, as member A of lab) ─────────────
ev_sub_start() {
    "$SIM" exec ctl1 -- sh -c "nohup runuser -u sim -- env HOME=/home/sim nats --server $NURL --no-context \
        --nkey /home/sim/.tether-A/keys/default.nk --connection-name tether-cli:$SID_A \
        sub 'tether.v2.sys.events' >$EVCAP 2>&1 & echo started" >/dev/null 2>&1
}
ev_ready() { "$SIM" exec ctl1 -- grep -q 'Subscribing' "$EVCAP" 2>/dev/null; }
ev_stop()  { "$SIM" exec ctl1 -- pkill -f 'sys.events' >/dev/null 2>&1 || true; }
# dedicated event predicates (multiple greps — JSON field order is not guaranteed). Only W's post-sub ctl
# join is in the capture (setup joins predate the sub; Arm R runs after ev_stop), so role=ctl is implied.
# R5: bind all fields to a SINGLE event line (piped greps are line-by-line) — not 3 independent greps over
# the whole capture (which could over-match if two events ever co-occur / Arm R leaks). sys.events are flat
# single-line JSON (audit.go:36-48). Ordering (ev_stop before Arm R) keeps the capture to W's two events.
_ev_pinfailed() { "$SIM" exec ctl1 -- sh -c "grep '\"type\":\"pin_failed\"' $EVCAP | grep '\"sid\":\"lab\"' | grep -q '\"role\":\"ctl\"'"; }
_ev_joined()    { "$SIM" exec ctl1 -- sh -c "grep '\"type\":\"member_joined\"' $EVCAP | grep '\"via\":\"pin\"' | grep -q '\"sid\":\"lab\"'"; }
# F1 (+ R2 residual): count the pin_failed events (the §E.6 counter's real trigger) with sid+role BOUND on
# the same JSON line — ≥10 proves the 10 wrong-PIN attempts all reached the Argon2-failure path for lab/ctl
# (were countable), not pre-blocked by some other refusal.
_ev_pinfailed_ge10() { _c=$("$SIM" exec ctl1 -- sh -c "grep '\"type\":\"pin_failed\"' $EVCAP | grep '\"sid\":\"lab\"' | grep -c '\"role\":\"ctl\"'" 2>/dev/null); [ "${_c:-0}" -ge 10 ]; }

# drill-local predicates (call the ident.sh functions; assert_ok runs these in a command-sub subshell that
# INHERITS the functions — unlike `sh -c "CTLH …"`, which spawns a fresh shell without them).
_a_sees_agt1() { CTLH A node ls 2>/dev/null | grep -qE "^agt1[[:space:]]+.*ONLINE"; }
_i1_persist()  { CTLH A ctx 2>/dev/null | grep -qw "$SID_A" && ! CTLH A ctx 2>/dev/null | grep -qw "$SID_B"; }
# R4: pin the CROSS-TENANT subject (.*s\.ops / .*s\.lab) — a bare 'Subscription to' would match a violation
# for ANY subject (e.g. an emptied member Sub.Allow regression). --count 1 returns on the first -ERR.
_i2_ab_sub()   { nats_as A "$SID_A" sub 'tether.v2.s.ops.audit.>' --timeout 3s --count 1 2>&1 | grep -qE 'Permissions Violation for Subscription to.*s\.ops'; }
_i2_ba_sub()   { nats_as B "$SID_B" sub 'tether.v2.s.lab.audit.>' --timeout 3s --count 1 2>&1 | grep -qE 'Permissions Violation for Subscription to.*s\.lab'; }
# R4 positive SUB controls: A CAN sub its OWN s.lab.audit.> (proves member Sub.Allow is non-empty, so the
# cross-tenant deny above is subject-scoped ACL, not a blanket "no sub" regression). No Permissions Violation.
# Time-bounded (nats_as_t): an ALLOWED sub with no message blocks forever, so we bound it to 4s and assert the
# ABSENCE of a violation (an allowed subscribe prints 'Subscribing …' then waits until timeout kills it).
_i2_posA_sub() { ! nats_as_t 4 A "$SID_A" sub 'tether.v2.s.lab.audit.>' 2>&1 | grep -qi 'Permissions Violation'; }
_i2_posB_sub() { ! nats_as_t 4 B "$SID_B" sub 'tether.v2.s.ops.audit.>' 2>&1 | grep -qi 'Permissions Violation'; }
_ts_lab()      { CTLHS T lab node ls 2>/dev/null | grep -qE '^agt1[[:space:]].*ONLINE' && ! CTLHS T lab node ls 2>/dev/null | grep -qE '^agt2[[:space:]]'; }
_ts_ops()      { CTLHS T ops node ls 2>/dev/null | grep -qE '^agt2[[:space:]].*ONLINE' && ! CTLHS T ops node ls 2>/dev/null | grep -qE '^agt1[[:space:]]'; }
_ts_file()     { CTLH T ctx 2>/dev/null | grep -qw "$SID_B"; }

drill_begin "80-session-isolation (N=1 + 2 agents: dual-tenant auth_callout ACL isolation)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 1 broker + 2 agents + 1 ctl"        "$SIM" up --brokers 1 --agents 2 --ctl 1
assert_ok "init brk1 (standalone → N=1 cluster)"  "$SIM" init brk1
assert_ok "N=1 leader floor (leader=brk1)"        sh -c "\"$SIM\" exec brk1 -- sh -c 'tether cluster status --json' | jq -e '.leader_id==\"brk1\" and (.health==\"DEGRADED\" or .health==\"HEALTHY_HA\")' >/dev/null"

# ── identities: A owns lab, B owns ops (distinct $TETHER_HOME = distinct nkey = distinct tenant) ──────
assert_ok "A creates session lab"                 CTLH A session create "$SID_A" --pin "$PIN_A" --nats-url "$NURL"
assert_ok "A activates lab (--broker persists broker_url)" CTLH A login -s "$SID_A" --pin "$PIN_A" --broker "$NURL"
assert_ok "B creates session ops"                 CTLH B session create "$SID_B" --pin "$PIN_B" --nats-url "$NURL"
assert_ok "B activates ops (--broker persists broker_url)" CTLH B login -s "$SID_B" --pin "$PIN_B" --broker "$NURL"
assert_ok "bind agt1 → lab"                       bind_agent agt1 "$SID_A" "$PIN_A"
assert_ok "bind agt2 → ops"                       bind_agent agt2 "$SID_B" "$PIN_B"
# T is a non-owner MEMBER of BOTH sessions (Oracle ③ + Arm TS)
assert_ok "T joins lab (non-owner member)"        CTLH T login -s "$SID_A" --pin "$PIN_A" --broker "$NURL"
assert_ok "T joins ops (non-owner member)"        CTLH T login -s "$SID_B" --pin "$PIN_B" --broker "$NURL"

# ── pre-flight: nats_as impersonation works (spike#4 confirmed; gate Oracle ②) ────────────────────────
assert_ok "nats_as pre-flight: A pub on its OWN lab subject (impersonation valid)" \
    nats_as A "$SID_A" pub 'tether.v2.s.lab.pty.x.in' probe

# ── Oracle ① — non-member activation CONNECT-refused + current_session NOT persisted ─────────────────
assert_ok      "I1-pos: A activates its own lab (positive control)"     CTLH A login -s "$SID_A" --pin "$PIN_A"
assert_refuses "I1-neg: A activating ops (non-member, no PIN) REFUSED at CONNECT" \
               "auth_callout rejected the connection" \
               CTLH A login -s "$SID_B"
assert_ok      "I1-persist: failed activation left ctx=lab (not ops)"  _i1_persist
assert_ok      "I1-broker: --broker alias persisted broker_url (node ls with no --nats-url works)" \
               _a_sees_agt1

# ── Oracle ② — bare-NATS cross-session ACL: bidirectional, pub+sub (protocol-layer Permissions Violation) ──
assert_ok      "I2-posA: A pub own lab subject (control)"   nats_as A "$SID_A" pub 'tether.v2.s.lab.pty.x.in' hi
assert_ok      "I2-posB: B pub own ops subject (control)"   nats_as B "$SID_B" pub 'tether.v2.s.ops.pty.x.in' hi
assert_ok      "I2-posA-sub: A CAN sub its own lab audit (R4 control — member Sub.Allow non-empty)"  _i2_posA_sub
assert_ok      "I2-posB-sub: B CAN sub its own ops audit (R4 control, symmetric)"                    _i2_posB_sub
assert_refuses "I2-AB-pub: A(lab)→ops PUB refused (Permissions Violation)" \
               "Permissions Violation for Publish to" \
               nats_as A "$SID_A" pub 'tether.v2.s.ops.pty.x.in' hi
assert_ok      "I2-AB-sub: A(lab)→ops SUB refused (Permissions Violation, verify OUTPUT)"  _i2_ab_sub
assert_refuses "I2-BA-pub: B(ops)→lab PUB refused (symmetric)" \
               "Permissions Violation for Publish to" \
               nats_as B "$SID_B" pub 'tether.v2.s.lab.pty.x.in' hi
assert_ok      "I2-BA-sub: B(ops)→lab SUB refused (symmetric, verify OUTPUT)"  _i2_ba_sub

# ── Oracle ②b — app-layer cross-tenant NODE isolation (uses the 2 agents) ────────────────────────────
assert_ok      "I2c-posA: A sees its own tenant node agt1 (control)"  _a_sees_agt1
assert_refuses "I2c-AB: A(lab) cannot reach agt2 (in ops) → node_not_found" \
               "node_not_found" \
               CTLH A exec agt2 -- true
assert_refuses "I2c-BA: B(ops) cannot reach agt1 (in lab) → node_not_found (symmetric)" \
               "node_not_found" \
               CTLH B exec agt1 -- true

# ── Oracle ③ — in-session non-owner reaches broker app-layer not_owner ───────────────────────────────
assert_ok      "I3-pos: T is a live lab MEMBER (control; so not_owner is app-layer)"  CTLH T node ls
assert_refuses "I3-rm: non-owner session rm → not_owner" \
               "not_owner" \
               CTLH T session rm "$SID_A"
assert_refuses "I3-up: non-owner node upgrade → not_owner (before url/sha validation)" \
               "not_owner" \
               CTLH T node upgrade agt1 --url https://x.invalid/x.tgz --sha256 0000000000000000000000000000000000000000000000000000000000000000

# ── Neg→pos + sys.events oracle (wrong-PIN → correct-PIN; pin_failed; member_joined) ─────────────────
ev_sub_start
assert_ok      "E-sub: authoritative sys.events observer live" \
               poll_until 15 1 "sys.events sub Subscribing" -- ev_ready
# E-neg: W (fresh non-member) wrong-PIN join → CONNECT refused
assert_refuses "E-neg: W wrong-PIN join REFUSED at CONNECT" \
               "auth_callout rejected the connection" \
               CTLH W login -s "$SID_A" --pin 999999 --broker "$NURL"
assert_ok      "E-pinfailed: pin_failed sys.event captured (sid=lab, role=ctl; the client-hidden reason)" \
               poll_until 15 1 "pin_failed event for lab/ctl" -- _ev_pinfailed
assert_ok      "E-pos: same identity correct-PIN join succeeds (no false lockout)" \
               CTLH W login -s "$SID_A" --pin "$PIN_A" --broker "$NURL"
assert_ok      "E-joined: member_joined sys.event captured (sid=lab, via=pin; W is the only post-sub join)" \
               poll_until 15 1 "member_joined event for lab via pin" -- _ev_joined
ev_stop   # E-clean: BEFORE Arm R (so Arm R never pollutes the E-arm capture)

# ── Arm R — PIN rate-limit black-box probe (探索→定格 预期-gotcha #25; INVERTED: success = the gap) ─────
# EXTERNAL-REVIEW F1: the §E.6 limiter counts on the Argon2 FAILURE branch (architecture §E.2: 失败 → 拒绝 +
# pin_failed + 按 E.6 计数); the threat is WRONG-PIN brute force. So the probe must fire WRONG PINs (the
# countable trigger), prove they reached the real auth path (pin_failed events), then show the SAME source's
# next CORRECT-PIN attempt STILL SUCCEEDS — a working ≤10/IP/min limiter would BLOCK the source after 10
# failures, refusing even the correct 11th. Its success = limiter ABSENT (#25). (Models the hermetic
# TestPINBruteForceNoLockout5Tries at deploy tier: N wrong + 1 correct-still-succeeds.)
ev_sub_start   # fresh capture (truncates $EVCAP) to COUNT the 10 pin_failed events
assert_ok "R-sub: fresh sys.events observer for the pin_failed count"  poll_until 15 1 "sub live" -- ev_ready
RF=0; i=1; while [ "$i" -le 10 ]; do
    CTLH "rf$i" login -s "$SID_A" --pin 111111 --broker "$NURL" >/dev/null 2>&1 || RF=$((RF+1)); i=$((i+1))
done
# (a) all 10 same-IP WRONG-PIN attempts were REFUSED (reached the real auth path = countable §E.6 attempts).
assert_ok "R-fails: all 10 same-IP WRONG-PIN attempts REFUSED (reached the Argon2-failure = countable path)"  sh -c "[ $RF -eq 10 ]"
# (b) BIND them to the limiter's real trigger: ≥10 pin_failed sys.events for lab/ctl captured.
assert_ok "R-pinfailed: ≥10 pin_failed sys.events captured (the §E.6 counter's trigger fired 10× — not pre-blocked)" \
    poll_until 15 1 "10 pin_failed events" -- _ev_pinfailed_ge10
ev_stop
# (c) THE inverted probe: after 10 same-IP failures, the 11th same-IP CORRECT-PIN attempt STILL SUCCEEDS —
# a ≤10/IP/min limiter would have blocked this source. Success = the recorded gap #25.
assert_ok "R-11th: after 10 same-IP wrong-PIN failures, the 11th same-IP CORRECT-PIN join STILL SUCCEEDS — PIN rate-limit ABSENT (§E.6 unimplemented; #25; FLIP to assert_refuses <rate-limit-sig> when the per-IP limiter lands)" \
    CTLH rok login -s "$SID_A" --pin "$PIN_A" --broker "$NURL"

# ── Arm TS — TETHER_SESSION dual-shell no-crosstalk ──────────────────────────────────────────────────
assert_ok      "TS-lab: shell pinned to lab sees agt1, NOT agt2"                 _ts_lab
assert_ok      "TS-ops: shell pinned to ops sees agt2, NOT agt1 (symmetric)"     _ts_ops
assert_ok      "TS-file: env unset falls back to file (last login = ops)"        _ts_file

# cleanup: kill any lingering sub
ev_stop
drill_end
