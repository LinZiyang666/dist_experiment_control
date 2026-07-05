#!/bin/sh
# 20-forcesingle-natsconf.sh — #20 RED: after online force-single the survivor's nats.conf stays
# clustered → JetStream 503-rots (plan §5.2). RED because there is no green code path today (open
# backlog): we assert the CURRENT broken behavior, signature-guarded so an alert-gate/other failure
# does NOT falsely "confirm" #20. Flip to a plain GREEN regression the day force-single de-clusters
# the survivor's nats.conf. Env: SIM, HERE, INSTANCE.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/setup-forcesingle.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab

drill_begin "#20 force-single → nats.conf stays clustered → JS 503 rot"

setup_forcesingle_n2

# record nats.conf mtime before force-single (to prove the conf is NOT touched).
MT0=$("$SIM" exec brk1 -- stat -c %Y /etc/tether/nats.conf 2>/dev/null)
assert_ok "force-single brk1 --dead brk2"                 "$SIM" force-single brk1 --dead brk2

# GREEN positive controls: the control plane is ALIVE — only JetStream rots. (Use JS-INDEPENDENT
# probes: node ls / exec go over core NATS + raft; even a tier-A push records to JS history/audit and
# would also 503, so it is NOT a valid "control plane alive" control here.)
assert_ok "control plane alive: node ls still lists agt1"  sh -c "$SIM ctl -- node ls | grep -q agt1"
assert_ok "control plane alive: exec on survivor works"    "$SIM" exec brk1 -- sh -c 'echo ok'

# RED mechanism proof: nats.conf STILL has a cluster{} block and its mtime is UNCHANGED across
# force-single (proves the lingering-conf mechanism, not an assumption).
assert_ok "nats.conf STILL has a cluster{} block"          sh -c "$SIM exec brk1 -- grep -qE '^cluster' /etc/tether/nats.conf"
assert_ok "nats.conf mtime UNCHANGED across force-single (conf not touched)" \
    sh -c "[ \"\$($SIM exec brk1 -- stat -c %Y /etc/tether/nats.conf 2>/dev/null)\" = \"$MT0\" ]"

# re-activate the ctl session (force-single can drop the ctl's connection state) so the push reaches
# the JetStream path rather than failing "no active session" (which would NOT match the #20 signature).
"$SIM" ctl -- login -s "$SID" --pin "$PIN" >/dev/null 2>&1 || true

# RED bug: tier-B push (JS Object Store) fails with bucket_create_failed 503 — the #20 symptom.
# --ack-alerts is MANDATORY (gateDestructive blocks under force_single_active BEFORE JetStream; without
# it the push fails at the ALERT GATE and falsely "confirms" #20). The signature requires the JS cause
# token so an alert-gate error / no-session surfaces as a HARD FAIL, not a false green.
assert_bug "tier-B push after force-single" "#20" "bucket_create_failed.*(503|10008)|(503|10008).*(bucket|JetStream)|JetStream system temporarily unavailable" \
    sh -c "$SIM exec ctl1 -- sh -c 'head -c 20000000 /dev/urandom > /tmp/tb.bin'; $SIM ctl -- push /tmp/tb.bin agt1:/tmp/tb.bin --ack-alerts"

# FLIP TO PURE-REGRESSION WHEN force-single de-clusters the survivor nats.conf.
drill_end
