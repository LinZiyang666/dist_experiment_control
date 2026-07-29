#!/bin/sh
# 20-forcesingle-natsconf.sh — #20/#12 GREEN regression: OFFLINE force-single AUTO-DE-CLUSTERS the
# survivor's nats.conf to standalone (the #20 fix — identity harvested from the survivor's cluster_nodes
# row, since a clustered conf's multi-user auth block can't self-identify) + PRUNES the abandoned peer
# (#12), so after the operator's JS-store reset + restart the data plane (tier-B) WORKS on the lone N=1
# survivor — no more silent 503-rot.
#
# WHY OFFLINE (not the sim's online cmd_force_single): (a) the #20 AUTO-de-cluster is the OFFLINE path (the
# broker is stopped, so `reconcile nats --to-standalone` would deadlock — offline renders it in-process);
# online deliberately leaves the conf for the operator's to-standalone (R3). (b) online force-single's
# quorum-loss dwell is reset every time the #23 `Restart=always` bounces the survivor after the peer dies,
# so it is timing-fragile in the container harness — offline (daemon stopped) has no dwell. Env: SIM, HERE,
# INSTANCE, SIMPIN.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/setup-forcesingle.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab
INST="${INSTANCE:-sim}"

drill_begin "#20/#12 OFFLINE force-single auto-de-clusters conf + prunes peer → tier-B WORKS at N=1 (GREEN)"

setup_forcesingle_n2

# brk2 must be provably dead (offline force-single HARD-REFUSES a peer that still answers :7400).
assert_ok "docker-kill brk2 (provably dead)" \
    sh -c "docker kill sim-$INST-brk2 >/dev/null 2>&1"

# round-5 §M1 (lint rule `sigpipe-truncation`): the de-cluster run must NOT be piped straight into `grep -q`
# — grep exits at its first match and SIGPIPEs the writer, killing the multi-step recovery MID-OPERATION.
# That is the exact drill-91 failure this very drill asserts the fix for: the run was cut off BEFORE its
# nats.conf de-cluster step. Capture to completion, then match the SAME signature over the captured output.
# Judgment is unchanged: tool rc still ignored, still `de-clustered to standalone` over its combined output.
# D1b (simcluster-accel): c6b9c9e made OFFLINE force-single REFUSE a non-empty clustered JS store without
# --reset-js (the store must be moved aside so a lone N=1 survivor can serve JS). This is CORRECT,
# journalled, forward-completing behaviour (cmd/tether/cluster_offline.go:202-250), NOT a bug — for an
# OFFLINE force-single the raft/DB phases commit BEFORE the gate, so a bare-verb "refusal" is post-commit
# and cannot be used as a clean probe; the operator re-runs the exact command with --reset-js to finish.
# The correct call-site sweep is therefore to pass --reset-js from the start, exactly as the GREEN drill
# 42 does (line 96). This was the SIXTH recurrence of "a contract change must sweep every call site".
_fs20() {
    _fs_out=$("$SIM" exec brk1 -- runuser -u tether -- python3 /opt/sim/pty-confirm.py brk1 -- \
        tether cluster recovery force-single --self-id brk1 --self-addr brk1:7400 --confirm-peers-dead brk2 --reset-js 2>&1)
    printf '%s\n' "$_fs_out" | grep -q 'de-clustered to standalone' && return 0
    # RE-EMIT ON FAILURE. Capturing the output, grepping it and dropping it makes this predicate's
    # failures evidence-free: the harness records _AS_OUT, which a capture-and-discard predicate leaves
    # EMPTY, so the drill log shows only "(want exit 0, got 1)". That is what happened on 2026-07-23 —
    # this drill red-ed on the new `--reset-js` gate and the refusal text naming the cause never reached
    # the log, while drill 91 (which let the same refusal through) printed it and was attributed in
    # minutes. The grep still decides the verdict; the output is now merely no longer thrown away.
    printf '%s\n' "$_fs_out" >&2
    return 1
}

# OFFLINE force-single on brk1: stop the daemon so the offline tool can take the disk, then run it as the
# data-dir owner (tether) — pty-fed typed confirm. The #20 fix AUTO-de-clusters nats.conf to standalone
# + prunes brk2, printing "de-clustered to standalone".
assert_ok "stop brk1 broker daemon (an operator stop — #23 Restart=always does NOT revive it)" \
    sh -c "$SIM exec brk1 -- systemctl stop tether-broker"
assert_ok "offline force-single brk1 auto-de-clusters + prunes (prints 'de-clustered to standalone')"  _fs20

# #20 FIX: the survivor's nats.conf is now STANDALONE (cluster{} block dropped) — the auto-de-cluster.
assert_ok "#20 FIXED: nats.conf auto-de-clustered to STANDALONE (no cluster{} block)" \
    sh -c "! $SIM exec brk1 -- grep -qE '^cluster' /etc/tether/nats.d/nats.conf"

# Internal review round-2 MINOR-1: mirror drill 42 FULLY — assert the PRODUCT (force-single --reset-js)
# moved the clustered store aside itself, and DELETE the hand-rolled `mv`. The old sim-side mv was a
# vestigial Mandate-④ concealment: with --reset-js it did nothing on the happy path, but it would MASK a
# future --reset-js regression that returned rc=0 without moving a data-bearing store (the sim mv would
# move the still-clustered store aside → nats restarts fresh → tier-B greens while the product silently
# failed to reset). The systemctl restart stays sim-side (Mandate ③).
assert_ok "#20 FIXED: force-single --reset-js MOVED the clustered JS store aside ITSELF (jetstream.force-single-bak.* present — the product verb, not the sim's old mv)" \
    sh -c "$SIM exec brk1 -- sh -c 'ls -d /var/lib/tether/jetstream.force-single-bak.* >/dev/null 2>&1'"
assert_ok "restart nats-server + broker (operator provisioning; the JS reset was the PRODUCT's job above)" \
    sh -c "$SIM exec brk1 -- sh -c 'systemctl restart nats-server; systemctl start tether-broker'"

# give the broker a moment to reconnect + JS to come up standalone.
sleep 6
"$SIM" ctl -- login -s "$SID" --pin "$PIN" >/dev/null 2>&1 || true

# #20 FIX (the payoff): tier-B push now WORKS on the lone N=1 survivor — standalone JS, no 503-rot.
assert_ok "#20 FIXED: tier-B push WORKS at N=1 after auto-de-cluster (standalone JS, no 503)" \
    sh -c "$SIM exec ctl1 -- sh -c 'head -c 12000000 /dev/urandom > /tmp/tb.bin'; $SIM ctl -- push /tmp/tb.bin agt1:/tmp/tb.bin --ack-alerts"

# #12 FIX: brk2 is pruned from the roster (no ghost VOTER lingering in the status node table).
assert_ok "#12 FIXED: abandoned brk2 pruned from the roster (no ghost VOTER)" \
    sh -c "! $SIM status --json | jq -e '.nodes[]? | select(.node_id==\"brk2\")' >/dev/null 2>&1"

# ── EXTERNAL review T1/B1: an INTERRUPTED recovery must FORWARD-COMPLETE from its on-disk intent ────
#
# The window B1 names is: the irreversible raft rewrite LANDED and the marker/epoch/prune did not.
# Before the durable intent existed that state was unrecoverable — `cluster status` did not report
# FORCE_SINGLE, the ctl destructive gate (QuorumLost || ForceSingleActive) was FULLY OPEN because the
# rewrite had just made QuorumLost false, and the documented repair (`re-run --online`) is refused by
# the arm gate precisely because the node now has leader contact: its own.
#
# This drill is where the injection is REACHABLE: it ends with a de-clustered, bootable N=1 whose
# JetStream works (asserted directly above), so the broker can actually be restarted and come back.
# (22-forcesingle-online deliberately leaves the conf CLUSTERED, which is the #35 startup crash-loop
# context — measured there at NRestarts=21 in a 45s window.)
#
# The intent and missing replicated facts are planted rather than raced into a sub-second SIGKILL
# window: what is under test is the RECOVERY, and the state an interruption leaves is exactly a
# rewritten {self} raft store + fsync'd intent + absent marker/epoch. The broker is stopped while
# sqlite is fault-injected, so no live writer is bypassed. This reproduces the crash boundary; it
# does not perform any recovery step on the product's behalf.
_t1_intent=/var/lib/tether/.force-single-online.intent
_t1_plant() {
    $SIM exec brk1 -- sh -c "cat > $_t1_intent <<JSON
{\"self_id\":\"brk1\",\"abandoned\":[\"brk2\"],\"epoch\":\"0123456789abcdef0123456789abcdef\",\"marked_at\":\"2026-07-29T00:00:00Z\"}
JSON
chown tether:tether $_t1_intent && chmod 600 $_t1_intent"
}
_t1_consumed()   { ! $SIM exec brk1 -- test -f "$_t1_intent"; }
_t1_up()         { $SIM exec brk1 -- sh -c 'tether cluster status --json 2>/dev/null | jq -e ".leader_id!=\"\"" >/dev/null' 2>/dev/null; }

assert_ok "T1/B1 setup: stop the broker before fault-injecting its post-rewrite durable state" \
    "$SIM" exec brk1 -- systemctl stop tether-broker
assert_ok "T1/B1 setup: delete marker+epoch to reproduce the exact crash window after raft rewrite" \
    "$SIM" exec brk1 -- runuser -u tether -- sqlite3 /var/lib/tether/tether.db \
        "DELETE FROM cluster_meta WHERE key IN ('force_single_active','force_single_epoch');"
assert_ok "T1/B1 setup: plant the durable intent an INTERRUPTED online force-single would have left" _t1_plant
assert_ok "T1/B1 non-vacuity: intent EXISTS and marker+epoch are both ABSENT before restart" \
    sh -c "$SIM exec brk1 -- sh -c 'test -f $_t1_intent && test \"\$(sqlite3 -readonly /var/lib/tether/tether.db \"SELECT count(*) FROM cluster_meta WHERE key IN ('\\''force_single_active'\\'','\\''force_single_epoch'\\'')\")\" = 0'"
assert_ok "T1/B1 setup: start brk1 so the intent is met on a fresh leadership acquisition" \
    sh -c "$SIM exec brk1 -- systemctl start tether-broker"
assert_ok "T1/B1 precondition: brk1 comes back up and has a leader (this drill's de-clustered N=1 is bootable, unlike drill 22's clustered survivor)" \
    poll_until 60 3 "brk1 leader after restart" -- _t1_up
if poll_until 90 3 "brk1 consumed the planted recovery intent" -- _t1_consumed; then
    assert_ok "T1/B1: an INTERRUPTED recovery forward-completes from its on-disk intent — the trigger is the INTENT, not the force_single_active marker it may never have written" sh -c "true"
    assert_ok "T1/B1: force_single_active is reported afterwards, so cluster status shows the emergency and the ctl destructive gate stays CLOSED" \
        sh -c "$SIM exec brk1 -- sh -c 'tether cluster status --json 2>/dev/null | jq -e \".force_single==true or (.health_label|test(\\\"FORCE.?SINGLE\\\";\\\"i\\\"))\" >/dev/null'"
    assert_ok "T1/B1: the exact epoch from the pre-rewrite intent was restored (no fresh token minted)" \
        sh -c "$SIM exec brk1 -- sh -c '[ \"\$(sqlite3 -readonly /var/lib/tether/tether.db \"SELECT value FROM cluster_meta WHERE key='\\''force_single_epoch'\\''\")\" = 0123456789abcdef0123456789abcdef ]'"
else
    log "DIAG T1/B1: broker log →"
    $SIM exec brk1 -- sh -c 'journalctl -u tether-broker --no-pager -n 500 2>/dev/null | grep -iE "intent|data dir|force.single" | tail -25' 2>&1 | sed 's/^/[diag t1] /'
    product_red "T1/B1 NOT recovered: brk1 restarted cleanly and has a leader, yet the planted force-single intent was still on disk 90s later — an interrupted recovery does not forward-complete, so the post-rewrite window remains unrecoverable (external review B1)"
fi

# GREEN regression since the #20 (offline auto-de-cluster from cluster_nodes identity) + #12 (abandoned
# prune) fixes. Was RED: force-single left the conf clustered → JS silently 503-rotted for days.
drill_end
