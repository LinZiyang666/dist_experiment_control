#!/bin/sh
# 21-smalldisk-tierb.sh — #21 RED: on a small-disk broker the hardcoded 8 GiB OBJ_xfer bucket
# reservation overshoots the store → tier-B (JetStream Object Store) is denied by JS storage admission
# (10047), while tier-A (inline) still works (plan §5.3). The small disk is modeled DETERMINISTICALLY
# with a size-capped tmpfs at the JS store_dir (never max_file_store — that subkey bricks reconcile;
# §9 OQ-5). Cap is > events+history (so the cluster works) but < the 8 GiB OBJ_xfer MaxBytes (so the
# FIRST tier-B bucket alone deterministically fails). Env: SIM, HERE, INSTANCE.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab

drill_begin "#21 small-disk: 8 GiB OBJ_xfer reservation denies tier-B"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 1 broker (JS store capped to 4g tmpfs) + agent + ctl"  "$SIM" up --brokers 1 --agents 1 --ctl 1 --cap-store 4g
assert_ok "init brk1 (N=1)"          "$SIM" init brk1
assert_ok "session + ctl login"      "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"          "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
# guard: the store cap must NOT be a max_file_store subkey (that bricks reconcile — §9 OQ-5).
assert_ok "no max_file_store in nats.conf (cap is tmpfs, not the forbidden subkey)"  sh -c "! $SIM exec brk1 -- grep -q max_file_store /etc/tether/nats.conf"
# GREEN control: the control plane is alive on the small disk (JS-independent — node ls over core NATS +
# raft). We do NOT assert a tier-A push here: even tier-A records to JS history/audit, which the tiny
# store also constrains, so it is not a clean "only the 8 GiB OBJ_xfer bucket fails" control.
assert_ok "control plane alive: node ls lists agt1"  sh -c "$SIM ctl -- node ls | grep -q agt1"
# RED bug: tier-B (OBJ_xfer bucket, 8 GiB MaxBytes) is denied by JS storage admission (10047).
# --ack-alerts keeps the signal on the JS storage-admission path: the near-ceiling 4g store can raise a
# disk_pressure alert whose gateDestructive block (not 10047) would otherwise false-RED this assertion.
assert_bug "tier-B push denied by JS storage admission" "#21" "bucket_create_failed.*(insufficient storage|10047)|insufficient storage resources|10047" \
    sh -c "$SIM exec ctl1 -- sh -c 'head -c 20000000 /dev/urandom > /tmp/tb.bin'; $SIM ctl -- push /tmp/tb.bin agt1:/tmp/tb.bin --ack-alerts"

# FLIP TO PURE-REGRESSION WHEN OBJ_xfer MaxBytes becomes disk-aware/configurable (#21 fix).
drill_end
