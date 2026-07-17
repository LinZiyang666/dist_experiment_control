#!/bin/sh
# 90-alerts-lifecycle.sh — S8 (N=3, GREEN): the operator alert lifecycle on the real store-backed alert
# plane. manual raise → member alert ls → severe banner on ps/node ls STDERR (stdout stays parseable) →
# ack (store-backed team-ack; ack does NOT clear the banner) → clear (idempotent) → real trigger kind
# broker_down (kill a FOLLOWER; the leader-gated observe loop writes it — N≥3 so the survivor keeps
# leadership, else the write is impossible: C1) → disk_pressure (capped store, M6) → quorum_lost `alert
# ack` interpret-REFUSED (B3).
#
# TRANSPORTS (source-verified cmd/tether/alert.go): `alert raise`/`alert clear` = broker LOCAL admin
# socket (operator tier; run `dexec -u tether $LDR -- tether alert raise …`); `alert ls`/`alert ack` =
# ctl-over-NATS (member read; run from ctl). broker_down/disk_pressure are INFO severity
# (observability.go:208 hardcodes AlertSeverityInfo) and renderBanner filters to `severe` only
# (d8_alerts.go) → they surface ONLY in `alert ls`, NEVER in the ps/node banner (C2). The severe banner
# is provable ONLY via `manual --severity severe`.
#
# FALSE-GREEN GUARDS (plan §9-90): clean-baseline gate before every arm (no pre-pulled broker_down/manual
# — a setup transient greening "alert exists after kill" for the wrong reason); severe banner ONLY via
# manual-severe; ack does NOT clear the banner (only clear removes it); store-backed ack via a committed
# re-read of acked_by; broker_down baseline liveness via the SAME nats-health signal the alert keys on;
# per-run SENTINEL so a stale manual can't green M1; `alert ack quorum_lost` needs the exact
# live-condition signature (a timeout / no-such-alert = wrong-reason).
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/drills/lib/cluster.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
SENT="SENTINEL90_$$"
CTL()   { "$SIM" ctl -- "$@"; }
RAISE() { dexec -u tether "$LDR" -- tether alert raise "$@"; }
CLEAR() { dexec -u tether "$LDR" -- tether alert clear "$@"; }
# member alert-ls JSON (ctl-over-NATS); print raw so callers can jq it.
ALS()   { CTL alert ls --json 2>/dev/null; }
# predicate: an active alert with dedup_key == $1 is present in the member view.
_alert_present() { ALS | jq -e --arg k "$1" '(.alerts // [])[]? | select(.dedup_key==$k)' >/dev/null 2>&1; }
# round-1 M4: the alert PLANE must have answered with valid JSON before ANY absence claim — a failed /
# garbage `alert ls` must NOT read as "no alert" (that fail-opens GREEN). Every `_*_absent` is guarded by
# _als_ok so absence means "the plane answered AND the key is not there", never "the query failed".
_alert_absent()  {
    _doc=$(ALS) || return 1
    printf '%s' "$_doc" | jq -e --arg k "$1" 'type=="object" and (((.alerts // [])|map(select(.dedup_key==$k))|length)==0)' >/dev/null 2>&1
}
# NB harness FUNCTIONS (dexec / poll_until / tcp_refused / node_kill / CTL) must be called DIRECTLY (they
# survive $(...) command-substitution) and NEVER wrapped in `sh -c "…"` (a new sh does not inherit them).
# Only BINARIES ($SIM / docker / jq via a pipe) may go inside `sh -c`. These drill-local predicates keep
# the function calls out of sh -c so poll_until can drive them.
_node_voter() { "$SIM" status --json 2>/dev/null | jq -e --arg n "$1" '.nodes[]? | select(.node_id==$n and .phase=="VOTER")' >/dev/null 2>&1; }
_bd_present() { CTL alert ls --json 2>/dev/null | jq -e --arg n "broker_down:$1" '(.alerts // [])[]? | select(.dedup_key==$n and .severity=="info")' >/dev/null 2>&1; }
_bd_absent()  { _alert_absent "broker_down:$1"; }
_bd_returned() { _node_voter "$1" && _bd_absent "$1"; }
_dp_present() { CTL alert ls --json 2>/dev/null | jq -e --arg n "disk_pressure:$1" '(.alerts // [])[]? | select(.dedup_key==$n and .kind=="disk_pressure" and .severity=="info")' >/dev/null 2>&1; }
_dp_absent()  { _alert_absent "disk_pressure:$1"; }
_lag_observed() {
    "$SIM" status --json 2>/dev/null | jq -e --arg n "$1" \
        '.nodes[]? | select(.node_id==$n and .reachable==true and (.applied_lag // 0)>64)' >/dev/null 2>&1
}
_write_raft_gap() {
    _i=1
    while [ "$_i" -le 36 ]; do
        dexec -u tether "$LDR" -- tether alert raise --kind manual --severity info --message "lag-fixture-$_i" >/dev/null || return 1
        dexec -u tether "$LDR" -- tether alert clear manual >/dev/null || return 1
        _i=$((_i+1))
    done
}

drill_begin "S8-90 alerts lifecycle: manual raise/ack/clear + broker_down + quorum_lost-ack refuse (N=3 GREEN)"

# ── SETUP: N=3 HA + 1 agent + ctl ──────────────────────────────────────────────────────────────────
assert_setup "grow_to_3 (N=3 HA baseline; single attempt)" grow_to_3 1 1 0
LDR=$(sim_leader) || setup_fail "no leader"
log "leader = $LDR"
# two distinct non-leader followers: FOLL_DOWN stays down (broker_down); FOLL_DISK returns (reserved M6).
FOLLS=$(list_nodes broker | grep -vx "$LDR")
FOLL_DOWN=$(printf '%s\n' "$FOLLS" | sed -n 1p)
FOLL_DISK=$(printf '%s\n' "$FOLLS" | sed -n 2p)
[ -n "$FOLL_DOWN" ] && [ -n "$FOLL_DISK" ] || setup_fail "need two non-leader followers (got '$FOLLS')"
log "followers: FOLL_DOWN=$FOLL_DOWN FOLL_DISK=$FOLL_DISK"
"$SIM" session "$SID" --pin "$PIN" >/dev/null 2>&1 || setup_fail "session create failed"
assert_setup "join agt1 for ps/banner coverage" "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"

# ── CLEAN-BASELINE GATE (anti-false-green): no active manual, no broker_down before any arm ──────────
assert_ok "clean baseline: NO active 'manual' alert before M1" sh -c "$SIM ctl -- alert ls --json 2>/dev/null | jq -e '((.alerts // [])|map(select(.dedup_key==\"manual\"))|length)==0' >/dev/null"
assert_ok "clean baseline: NO active broker_down before M5" sh -c "$SIM ctl -- alert ls --json 2>/dev/null | jq -e '((.alerts // [])|map(select(.kind==\"broker_down\"))|length)==0' >/dev/null"

# ── M1 raise (admin socket, operator) → member alert ls sees it ─────────────────────────────────────
assert_ok "M1 raise manual/severe on the leader admin socket ($SENT)" \
    RAISE --kind manual --severity severe --message "$SENT"
assert_ok "M1 member alert ls (ctl-over-NATS) sees dedup_key=manual severity=severe msg=$SENT" \
    sh -c "$SIM ctl -- alert ls --json 2>/dev/null | jq -e --arg s '$SENT' '(.alerts // [])[]? | select(.dedup_key==\"manual\" and .severity==\"severe\" and (.message|test(\$s)))' >/dev/null"

# ── M2 severe banner on ps/node ls STDERR, stdout stays parseable ────────────────────────────────────
# ps stderr carries the ‼ ALERT banner; stdout must be clean (no ‼). --json suppresses the banner entirely.
assert_ok "M2 severe banner on 'ps' STDERR (‼ ALERT [manual] …$SENT)" \
    sh -c "$SIM ctl -- ps 2>&1 1>/dev/null | grep -qE '‼|ALERT.*manual|$SENT'"
assert_ok "M2 'ps' STDOUT stays parseable (no ‼ banner on stdout)" \
    sh -c "! $SIM ctl -- ps 2>/dev/null | grep -q '‼'"
assert_ok "M2 severe banner also on 'node ls' STDERR" \
    sh -c "$SIM ctl -- node ls 2>&1 1>/dev/null | grep -qE '‼|ALERT.*manual'"
assert_ok "M2 --json suppresses the banner (no ‼ in ps --json either stream)" \
    sh -c "! $SIM ctl -- ps --json 2>&1 | grep -q '‼'"

# ── M3 ack = store-backed team-ack; ack does NOT clear the banner ────────────────────────────────────
assert_ok "M3 alert ack manual (ctl-over-NATS, store-backed)" CTL alert ack manual
assert_ok "M3 acked_by committed on a fresh re-read (length>0)" \
    sh -c "$SIM ctl -- alert ls --json 2>/dev/null | jq -e '(.alerts // [])[]? | select(.dedup_key==\"manual\") | (.acked_by // [] | length) > 0' >/dev/null"
# FG (subtle): ack suppresses the inline prompt but the severe banner STILL renders on ps stderr — only
# `clear` removes it. A drill asserting "banner gone after ack" would test a false promise.
assert_ok "M3 FG: severe banner STILL renders on ps stderr AFTER ack (ack≠clear)" \
    sh -c "$SIM ctl -- ps 2>&1 1>/dev/null | grep -qE '‼|ALERT.*manual'"

# ── M4 clear idempotent → banner gone ───────────────────────────────────────────────────────────────
assert_ok "M4 alert clear manual (admin socket)" CLEAR manual
assert_ok "M4 manual no longer active in member view" sh -c "$SIM ctl -- alert ls --json 2>/dev/null | jq -e '((.alerts // [])|map(select(.dedup_key==\"manual\"))|length)==0' >/dev/null"
assert_ok "M4 second clear is idempotent (exit 0 again)" CLEAR manual
assert_ok "M4 post-clear: NO ‼ banner on ps stderr" sh -c "! $SIM ctl -- ps 2>&1 1>/dev/null | grep -qE '‼ ALERT \\[manual\\]'"

# ── M5 kill a FOLLOWER → broker_down (5 elements) ────────────────────────────────────────────────────
# ① baseline: FOLL_DOWN is a live VOTER (leader status). (predicates called DIRECTLY — never via sh -c.)
assert_ok "M5① baseline: $FOLL_DOWN is a live VOTER" _node_voter "$FOLL_DOWN"
# ③ inject: kill FOLL_DOWN (keep container), prove dead.
node_kill "$FOLL_DOWN"
assert_ok "M5③ $FOLL_DOWN raft :7400 connection-refused (provably dead)" \
    poll_until 30 2 "$FOLL_DOWN :7400 refused" -- tcp_refused "$FOLL_DOWN" 7400
# ④ semantic oracle: the leader-gated observe loop writes broker_down:$FOLL_DOWN (INFO), member-readable.
assert_ok "M5④ broker_down:$FOLL_DOWN raised (info) in member alert ls within 45s" \
    poll_until 45 3 "broker_down:$FOLL_DOWN" -- _bd_present "$FOLL_DOWN"
# ⑤ return: FOLL_DOWN back → broker_down clears (deferred to end; see cleanup arm below).

# ── M7 `alert ack quorum_lost` interpret-REFUSED (B3, live-condition, no real quorum loss needed) ─────
assert_refuses "M7 alert ack quorum_lost → live-condition refuse (NOT a store-backed dedup_key)" \
    "LIVE cluster-health condition|NOT a store-backed alert|cannot ack" \
    sh -c "$SIM ctl -- alert ack quorum_lost 2>&1"
assert_refuses "M7 alert ack force_single_active → same live-condition refuse" \
    "LIVE cluster-health condition|NOT a store-backed alert|cannot ack" \
    sh -c "$SIM ctl -- alert ack force_single_active 2>&1"

# Restore the M5 victim BEFORE isolating the other follower. Leaving FOLL_DOWN dead while dropping
# FOLL_DISK's raft ingress removes both followers from the write quorum, so the intended lag-producing
# writes cannot commit and the fixture tests quorum loss instead of raft_lag.
node_start "$FOLL_DOWN"
assert_ok "M5⑤ $FOLL_DOWN returns to VOTER + broker_down:$FOLL_DOWN clears before M9" \
    poll_until 120 4 "VOTER and broker_down clears in the same predicate" -- _bd_returned "$FOLL_DOWN"

# ── M9 raft_lag — isolate only one follower's inbound raft port, keep its NATS health live ──────────
# clsact ingress drops :7400 without touching :4222.  The other two voters retain quorum, so 72 real
# alert transitions advance the leader >64 command entries while the isolated follower still answers
# cluster-health.  This discriminates raft_lag from broker_down and covers both raise and clear.
LAG_FOLL=$FOLL_DISK
assert_setup "M9 install follower-only ingress raft drop on $LAG_FOLL (NATS remains reachable)" \
    "$SIM" exec "$LAG_FOLL" -- sh -eu -c 'tc qdisc add dev eth0 clsact; tc filter add dev eth0 ingress protocol ip prio 1 u32 match ip dport 7400 0xffff action drop'
assert_setup "M9 commit 72 real raft alert transitions while $LAG_FOLL cannot receive raft" _write_raft_gap
assert_ok "M9 $LAG_FOLL still answers health but trails the leader by >64 entries" \
    poll_until 45 3 "$LAG_FOLL reachable with applied_lag>64" -- _lag_observed "$LAG_FOLL"
assert_ok "M9 raft_lag:$LAG_FOLL raises (not broker_down)" \
    poll_until 45 3 "raft_lag:$LAG_FOLL" -- _alert_present "raft_lag:$LAG_FOLL"
assert_ok "M9 lag fixture did not misclassify the live responder as broker_down" _alert_absent "broker_down:$LAG_FOLL"
assert_setup "M9 remove follower raft drop" "$SIM" exec "$LAG_FOLL" -- tc qdisc del dev eth0 clsact
assert_ok "M9 follower catches up and raft_lag:$LAG_FOLL clears" \
    poll_until 90 3 "$LAG_FOLL caught up + alert clear" -- sh -c \
    "$SIM status --json 2>/dev/null | jq -e --arg n '$LAG_FOLL' '.nodes[]?|select(.node_id==\$n and .reachable==true and (.applied_lag // 0)<=64)' >/dev/null && $SIM ctl -- alert ls --json 2>/dev/null | jq -e --arg k 'raft_lag:$LAG_FOLL' '((.alerts // [])|map(select(.dedup_key==\$k))|length)==0' >/dev/null"

# ── M5⑤ return: FOLL_DOWN back → broker_down auto-clears (control-source recovery) ───────────────────

# ── M6 disk_pressure — capped-store sub-fixture (5 elements; explore→pin, OQ-5) ──────────────────────
# disk_pressure needs cluster-mode + a live leader. grow_to_3 is UNCAPPED (filling >80% of the server's
# filesystem is infeasible), so re-provision a capped N=2 and fill the FOLLOWER. This proves the real
# follower→leader VerbAlertSignal path instead of reducing fidelity to a self-forwarding N=1 fixture.
# ① clean baseline (no disk_pressure) ② member alert ls ③ fill the capped data-dir tmpfs >80% + bounce the broker
# (startup tick re-samples, disk.go:102) ④ disk_pressure:brk1 raised (info), dedup_key node-scoped
# ⑤ remove ballast → clears. If the capped N=1 won't provision (OQ-5) → NOT-COVERED-with-reason (hermetic).
assert_setup "M6 reset prior topology" "$SIM" nuke
assert_setup "M6 build capped 3 GiB N=2 fixture" "$SIM" up --brokers 2 --agents 0 --ctl 1 --cap-store 3g
assert_setup "M6 initialize capped fixture" "$SIM" init brk1
assert_setup "M6 grow brk2 so disk pressure must forward across brokers" "$SIM" grow brk2
LDR=$(sim_leader) || setup_fail "M6 no leader"
DISK_FOLL=$(list_nodes broker | grep -vx "$LDR" | head -1)
[ -n "$DISK_FOLL" ] || setup_fail "M6 no non-leader disk fixture"
assert_setup "M6 create member session for alert reads" "$SIM" session "$SID" --pin "$PIN"
assert_ok "M6① clean baseline: NO disk_pressure:$DISK_FOLL before fill" _dp_absent "$DISK_FOLL"
assert_ok "M6③ allocate ballast and prove the SAME JS filesystem is above 80%" \
    "$SIM" exec "$DISK_FOLL" -- sh -eu -c 'fallocate -l 2700M /var/lib/tether/jetstream/ballast; p=$(df -P /var/lib/tether/jetstream | awk "NR==2{gsub(/%/,\"\",\$5);print \$5}"); test "$p" -gt 80'
assert_ok "M6③ restart broker successfully to trigger the documented startup disk sample" \
    "$SIM" exec "$DISK_FOLL" -- sh -eu -c 'systemctl restart tether-broker; test "$(systemctl is-active tether-broker)" = active'
if poll_until 90 3 "disk_pressure:$DISK_FOLL exact dedup key" -- _dp_present "$DISK_FOLL"; then
    _as_pass "M6④ exact disk_pressure:$DISK_FOLL/info alert raised through follower→leader forwarding after proven >80% fill + startup sample"
else
    product_red "#39 disk_pressure startup sample failed to raise exact disk_pressure:$DISK_FOLL within 90s after a proven >80% fill and successful follower restart"
fi
assert_ok "M6⑤ remove ballast and restart successfully for the clear sample" \
    "$SIM" exec "$DISK_FOLL" -- sh -eu -c 'rm -f /var/lib/tether/jetstream/ballast; systemctl restart tether-broker; test "$(systemctl is-active tether-broker)" = active'
assert_ok "M6⑤ exact disk_pressure:$DISK_FOLL clears after recovery" poll_until 90 4 "disk_pressure:$DISK_FOLL clears" -- _dp_absent "$DISK_FOLL"

# Stage-C M14: below_quorum's "#31-gated retire" blocker was FALSE — below_quorum fires on ANY nv==2,
# health-unconditional (alert_reconcile.go:253 `below := nv==2`, leader-gated only :176), and
# setup_forcesingle_n2 already builds nv==2 with NO retire. It is a WORKING alert (a missing cheap-GREEN,
# not a hidden RED) — a below_quorum N=2 leg is a follow-up (SB-90). raft_lag stays NOT-COVERED (a
# deterministic >64-entry follower lag is genuinely not inducible in-sim). Both hermetic-tested meanwhile.
assert_setup "M8 reset before the below_quorum N=2 leg" "$SIM" nuke
assert_setup "M8 provision three brokers (grow first to N=2, then N=3 clear control)" "$SIM" up --brokers 3 --agents 0 --ctl 1
assert_setup "M8 initialize brk1" "$SIM" init brk1
assert_setup "M8 grow brk2 to form exactly N=2" "$SIM" grow brk2
assert_setup "M8 create member session for N=2 alert reads" "$SIM" session "$SID" --pin "$PIN"
assert_ok "M8 below_quorum raises at exactly N=2" poll_until 45 3 "below_quorum raised" -- _alert_present below_quorum
assert_setup "M8 grow brk3 to restore N=3 fault tolerance" "$SIM" grow brk3
assert_ok "M8 below_quorum clears after the same topology reaches N=3" poll_until 45 3 "below_quorum clears" -- _alert_absent below_quorum
drill_end
