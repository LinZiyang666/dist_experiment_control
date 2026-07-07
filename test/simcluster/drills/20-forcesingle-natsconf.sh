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

# OFFLINE force-single on brk1: stop the daemon so the offline tool can take the disk, then run it as the
# data-dir owner (tether) — pty-fed typed confirm. The #20 fix AUTO-de-clusters nats.conf to standalone
# + prunes brk2, printing "de-clustered to standalone".
assert_ok "stop brk1 broker daemon (an operator stop — #23 Restart=always does NOT revive it)" \
    sh -c "$SIM exec brk1 -- systemctl stop tether-broker"
assert_ok "offline force-single brk1 auto-de-clusters + prunes (prints 'de-clustered to standalone')" \
    sh -c "$SIM exec brk1 -- runuser -u tether -- python3 /opt/sim/pty-confirm.py brk1 -- tether cluster recovery force-single --self-id brk1 --self-addr brk1:7400 --confirm-peers-dead brk2 2>&1 | grep -q 'de-clustered to standalone'"

# #20 FIX: the survivor's nats.conf is now STANDALONE (cluster{} block dropped) — the auto-de-cluster.
assert_ok "#20 FIXED: nats.conf auto-de-clustered to STANDALONE (no cluster{} block)" \
    sh -c "! $SIM exec brk1 -- grep -qE '^cluster' /etc/tether/nats.d/nats.conf"

# Operator provisioning (the sim's job, not tether's): RESET the clustered JS store (mv, not rm — the #20
# warning printed the exact reset) + restart nats-server (loads the standalone conf) + restart the broker.
assert_ok "reset JS store + restart nats-server + broker (provisioning)" \
    sh -c "$SIM exec brk1 -- sh -c 'mv /var/lib/tether/jetstream /var/lib/tether/jetstream.bak.\$(date +%s) 2>/dev/null; systemctl restart nats-server; systemctl start tether-broker'"

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
