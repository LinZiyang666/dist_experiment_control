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

# GREEN regression since the #20 (offline auto-de-cluster from cluster_nodes identity) + #12 (abandoned
# prune) fixes. Was RED: force-single left the conf clustered → JS silently 503-rotted for days.
drill_end
