# drills/lib/events.sh — sys.events core-sub observer (G-C; s7-s9-plan §1.2-(6)).
# POSIX sh. Sourced by drills 94/95/96. Depends on the drill's $SIM.
#
# EXTRACTED, NOT INVENTED. This generalizes the live recipe already proven in
# drills/81-admin-evict-session-rm.sh:62-66 (and 80:42-53) — an owner ctl subscribing to the global
# sys.events subject with its own nkey.
#
# WHY THIS EXISTS AT ALL (it overturns a carve-out G-A and G-B both inherited).
# G-A/G-B declared every raw `pubSysEvent` kind NOT-COVERED-as-unreadable because "admin has no events
# reader". The CONCLUSION about `tether cluster admin` is true (cmd/tether/admin.go has only
# sessions/nodes/audit/evict), but the INFERENCE was wrong: it generalized a JetStream-API permission
# (`nats stream ls` is denied) into a core-subscribe permission. In fact
#   internal/auth/permissions.go:147  PermissionsForActivatedMember  Sub allow includes tether.v2.sys.events
#   internal/auth/permissions.go:36   PermissionsForUnactivated      likewise
# — a global subject with no sid wildcard. An activated member's ctl credentials CAN core-sub every
# sys.events kind. Drills 80/81 have been doing exactly that in production for two batches.
#
# So G-C splits the question three ways instead of applying one blunt rule (s7-s9-plan §1.4-R-EVENTS):
#   (1) does the product EMIT the event?          -> member core-sub can see it -> ASSERT IT.
#   (2) is there a first-class operator CLI reader? -> no -> registered separately as DOC-26.
#   (3) G-A's proxy/rehome NOT-COVERED rows        -> the conclusion may still stand, but the stated
#       reason is false at source level -> handed to G-C-SWEEP for per-row re-adjudication, NOT silently
#       rewritten here.
#
# Corollary already closed in source (SB-EV2): rehome kinds are readable too —
# internal/broker/rehome_events.go:9-11 says verbatim that all emits go through b.pubSysEvent onto the
# existing proto.SubjSysEvents. So `home_reassign_*` / `rehome_stalled` / `broker_down_rehome_summary`
# must NOT be judged unreadable (inventory row 54).
#
# HONEST LIMITS — write these into any arm that uses this:
#   * The subscription is a LIVE core sub: it sees only what is published WHILE it is up. Restarting
#     nats-server drops it (95's EV arm therefore hangs off T1/T2 only, never T3).
#   * Absence of an event is NOT evidence unless the sub is proven ready (ev_ready) AND a positive
#     control event was captured in the same window. Use ev_seen for presence; for absence, pair it.
#   * It is CORROBORATION. Never gate a drill's spine on it.

# ev_sub_start <ctl-node> <sid> <capture-file> : start a background core-sub of tether.v2.sys.events on
# <ctl-node>, capturing raw JSON lines to <capture-file> inside that container.
ev_sub_start() {
    _es_ctl=$1; _es_sid=$2; _es_cap=$3
    "$SIM" exec "$_es_ctl" -- sh -c "nohup runuser -u sim -- env HOME=/home/sim nats --server ${EV_NURL:?EV_NURL must be set to the broker nats url} --no-context --nkey /home/sim/.tether/keys/default.nk --connection-name tether-cli:$_es_sid sub 'tether.v2.sys.events' >$_es_cap 2>&1 & echo ok" >/dev/null 2>&1
}

# ev_ready <ctl-node> <capture-file> : predicate — the sub is actually established. Poll on this BEFORE
# injecting anything; otherwise a missed event is indistinguishable from a late subscription.
ev_ready() { "$SIM" exec "$1" -- grep -q 'Subscribing' "$2" 2>/dev/null; }

# ev_count <ctl-node> <capture-file> <kind> : print how many events of "type":"<kind>" were captured.
# Use a strictly-increasing count across an injection rather than a bare presence check — a kind that was
# already in the capture from setup would otherwise make the arm pass for the wrong reason.
ev_count() {
    # grep -c always PRINTS a count (0 on no match) but EXITS 1 on no match; a `|| echo 0` would append a
    # SECOND 0 -> "0\n0" (Stage-C minor). Drop the fallback; force numeric with a trailing arithmetic guard.
    _ec=$("$SIM" exec "$1" -- sh -c "grep -c '\"type\":\"$3\"' '$2' 2>/dev/null" | tr -d '\r' | head -1)
    printf '%s' "${_ec:-0}"
}

# ev_seen <ctl-node> <capture-file> <kind> : predicate — at least one event of <kind> was captured.
ev_seen() { "$SIM" exec "$1" -- grep -q "\"type\":\"$3\"" "$2" 2>/dev/null; }

# ev_seen_field <ctl-node> <capture-file> <kind> <json-fragment> : predicate — an event of <kind> whose
# raw line also contains <json-fragment> (e.g. '"sid":"lab"'). Mirrors 81's _ev_destroyed.
ev_seen_field() {
    "$SIM" exec "$1" -- sh -c "grep '\"type\":\"$3\"' '$2' 2>/dev/null | grep -q '$4'"
}

# ev_stop <ctl-node> : kill the background sub. Safe to call from the drill's single EXIT trap.
ev_stop() { "$SIM" exec "$1" -- pkill -f 'sys.events' >/dev/null 2>&1 || true; }
