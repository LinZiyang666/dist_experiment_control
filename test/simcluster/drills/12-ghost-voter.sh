#!/bin/sh
# 12-ghost-voter.sh — #12 GREEN regression: the old "three-non deadlock" is ROOT-CAUSED AWAY. force-single
# now AUTO-PRUNES the abandoned peer (#12), so it never lingers as a phase==VOTER ghost, and the three
# online removal paths that used to deadlock have nothing to refuse.
#
# Uses OFFLINE force-single (RELIABLE): online force-single's quorum-loss dwell is reset every time the #23
# `Restart=always` bounces the survivor after the peer dies, so online is timing-fragile in the container
# harness (see drill 20 — same reason it went offline). Offline (daemon stopped) has no dwell.
#
# The UPGRADE-LEFTOVER-ghost path — a VOTER roster row ABSENT from the committed raft config, left by a
# pre-#12 binary — is removed via `recovery node remove` (ghost passthrough). That legacy ghost CANNOT be
# manufactured on this deploy tier (the container has no sqlite3 and there is no old binary), so it is
# covered HERMETICALLY by internal/broker: TestG2RemoveNodeGhostPassthrough (passthrough delete + ownership
# guard + in-config-VOTER still refused + leader-gate) and TestFilterGhostPeersDropsNotInConfig (the
# upgrade migration guard). Env: SIM, HERE, INSTANCE, SIMPIN.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/setup-forcesingle.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab
INST="${INSTANCE:-sim}"

drill_begin "#12 GREEN: force-single AUTO-PRUNES the abandoned peer → no ghost VOTER, three-non deadlock gone"

setup_forcesingle_n2

# brk2 must be provably dead (offline force-single HARD-REFUSES a peer that still answers :7400).
assert_ok "docker-kill brk2 (provably dead)" \
    sh -c "docker kill sim-$INST-brk2 >/dev/null 2>&1"

# round-5 §M1 (lint rule `sigpipe-truncation`): the offline recovery run must NOT be piped straight into
# `grep -q` — grep exits at its first match and SIGPIPEs the writer, which can kill a multi-step recovery
# MID-OPERATION (proven on drill 91: the run died before its nats.conf step and the drill blamed the
# product). Capture the run to completion first, then match the SAME signature over the captured output.
# Judgment is unchanged: still the tool's rc ignored, still `single-voter cluster` over its combined output.
_fs12() {
    _fs_out=$("$SIM" exec brk1 -- runuser -u tether -- python3 /opt/sim/pty-confirm.py brk1 -- \
        tether cluster recovery force-single --self-id brk1 --self-addr brk1:7400 --confirm-peers-dead brk2 2>&1)
    printf '%s\n' "$_fs_out" | grep -q 'single-voter cluster'
}

# OFFLINE force-single on brk1 (reliable — no online dwell/leadership race). #12: it AUTO-PRUNES brk2.
assert_ok "stop brk1 broker daemon (an operator stop — #23 Restart=always does NOT revive it)" \
    sh -c "$SIM exec brk1 -- systemctl stop tether-broker"
assert_ok "offline force-single brk1 (auto-prunes the abandoned brk2)"  _fs12
assert_ok "restart nats-server + broker (provisioning)" \
    sh -c "$SIM exec brk1 -- sh -c 'mv /var/lib/tether/jetstream /var/lib/tether/jetstream.bak.\$(date +%s) 2>/dev/null; systemctl restart nats-server; systemctl start tether-broker'"
sleep 6

# #12 FIX (the root cause): brk2 is PRUNED — it never lingers as a phase==VOTER ghost, so the "three-non
# deadlock" (all three online removal paths refuse an undeletable VOTER ghost) simply cannot arise.
assert_ok "#12 FIXED: brk2 auto-pruned from the roster (no ghost VOTER — the RED deadlock premise is gone)" \
    sh -c "! $SIM exec brk1 -- sh -c \"tether cluster status --json 2>/dev/null | jq -e '.nodes[]?|select((.node_id//.id//.name)==\\\"brk2\\\")' >/dev/null\""

# and the removal path that USED to deadlock now cleanly reports the row is gone (a clean 'no such roster
# node', NOT the old three-non phase-gate refusal on an undeletable VOTER).
assert_ok "#12 FIXED: recovery node remove of the pruned brk2 reports 'no such roster node' (not a deadlock refusal)" \
    sh -c "$SIM exec brk1 -- env TETHER_CONFIRM_NODE_ID=brk2 tether cluster recovery node remove brk2 --manual --confirm-node-id brk2 2>&1 | grep -q 'no such roster node'"

# batch C regression guard. C1 added an OpKindForceSingleFinalize retry, but ONLY on the ONLINE path
# and ONLY when its synchronous prune fails. This drill exercises the OFFLINE path, where the prune is
# done in-process by clusteroffline.ForceSingle against the stopped broker's disk — the finalize
# machinery does not participate at all.
#
# So the honest deploy-tier assertion here is that batch C did NOT reach into this path: no operation
# row appears, and the ghost outcome is byte-for-byte what it was before. (Injecting a prune failure to
# exercise the retry belongs on the ONLINE path, and lives in 22-forcesingle-online.sh — manufacturing
# one here would be testing a mechanism this path never invokes.)
assert_ok "batch-C: the OFFLINE force-single path creates NO force_single_finalize op (the retry op is ONLINE-only, and only on a failed prune)" \
    sh -c "! $SIM exec brk1 -- sh -c 'tether cluster ops ls --json 2>/dev/null | grep -q force_single_finalize'"

# GREEN regression since the #12 fix (force-single auto-prune). Was RED: force-single left brk2 phase==VOTER
# and all three online removal paths refused it (the three-non deadlock). The upgrade-leftover ghost
# (VOTER-not-in-committed-config from a pre-#12 binary) → `recovery node remove` passthrough is covered by
# hermetic TestG2RemoveNodeGhostPassthrough (this deploy tier has no sqlite3 + no old binary to manufacture
# that legacy ghost — verified 2026-07-07).
drill_end
