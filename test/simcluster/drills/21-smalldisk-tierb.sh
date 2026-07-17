#!/bin/sh
# 21-smalldisk-tierb.sh — #21 RED: on a small-disk broker the hardcoded 8 GiB OBJ_xfer bucket
# reservation overshoots the store → tier-B (JetStream Object Store) is denied by JS storage admission
# (10047), while tier-A (inline) still works (plan §5.3). The small disk is modeled DETERMINISTICALLY
# with a size-capped tmpfs at the broker data-dir filesystem (never max_file_store — that subkey bricks reconcile;
# §9 OQ-5). Cap is > events+history (so the cluster works) but < the 8 GiB OBJ_xfer MaxBytes (so the
# FIRST tier-B bucket alone deterministically fails). Env: SIM, HERE, INSTANCE.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab

drill_begin "#21 small-disk: disk-aware OBJ_xfer fits under the store → tier-B WORKS (GREEN regression)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 1 broker (JS filesystem capped to 4g tmpfs) + agent + ctl"  "$SIM" up --brokers 1 --agents 1 --ctl 1 --cap-store 4g
assert_ok "init brk1 (N=1)"          "$SIM" init brk1
assert_ok "session + ctl login"      "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"          "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
# guard: the store cap must NOT be a max_file_store subkey (that bricks reconcile — §9 OQ-5).
assert_ok "no max_file_store in nats.conf (cap is tmpfs, not the forbidden subkey)"  sh -c "! $SIM exec brk1 -- grep -q max_file_store /etc/tether/nats.d/nats.conf"
# GREEN control: the control plane is alive on the small disk (JS-independent — node ls over core NATS +
# raft). We do NOT assert a tier-A push here: even tier-A records to JS history/audit, which the tiny
# store also constrains, so it is not a clean "only the 8 GiB OBJ_xfer bucket fails" control.
assert_ok "control plane alive: node ls lists agt1"  sh -c "$SIM ctl -- node ls | grep -q agt1"
# GREEN regression (#21 fixed): the OBJ_xfer bucket MaxBytes is now DISK-AWARE — on a 4g store it sizes to
# a fraction of (ceiling - events/history) instead of a hardcoded 8 GiB, so the tier-B bucket create is
# ADMITTED and the push SUCCEEDS. --ack-alerts keeps a near-ceiling disk_pressure alert from false-REDding.
assert_ok "tier-B push SUCCEEDS on the small disk (disk-aware OBJ_xfer; was #21 RED: 8 GiB hardcode → 10047)" \
    sh -c "$SIM exec ctl1 -- sh -c 'head -c 20000000 /dev/urandom > /tmp/tb.bin'; $SIM ctl -- push /tmp/tb.bin agt1:/tmp/tb.bin --ack-alerts"
# mechanism anchor: the file really landed on the agent (tier-B round-trip completed, not just a queued no-op).
assert_ok "tier-B file present on agt1 (round-trip completed, not a queued no-op)" \
    sh -c "$SIM exec agt1 -- test -s /tmp/tb.bin"

# GREEN regression since the #21 fix (disk-aware OBJ_xfer MaxBytes). Was RED: 8 GiB hardcode → JS storage
# admission 10047 on the 4g store. The `! grep max_file_store` guard above must STAY green (that subkey
# bricks reconcile — the fix is bucket-side sizing, never a rendered max_file_store).
drill_end
