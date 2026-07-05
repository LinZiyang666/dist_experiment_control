#!/bin/sh
# 12-ghost-voter.sh — #12 RED: after online force-single the ejected peer is left phase==VOTER in the
# roster (raft config dropped it, roster row didn't) and ALL THREE online removal paths refuse it — the
# "three-non" deadlock (plan §5.4). RED (open backlog). The refusal SIGNATURES are captured broad; if a
# path fails for an UNDOCUMENTED reason the guard HARD-FAILs (not a false green). Env: SIM, HERE, INSTANCE.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/setup-forcesingle.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab

drill_begin "#12 force-single ghost VOTER — three-non deadlock"

setup_forcesingle_n2
assert_ok "force-single brk1 --dead brk2"  "$SIM" force-single brk1 --dead brk2

# GREEN control: the ghost is visible — brk2 still listed phase==VOTER in the survivor's roster.
assert_ok "ghost brk2 still listed phase==VOTER in the roster" \
    sh -c "$SIM exec brk1 -- tether cluster status --json 2>/dev/null | jq -e '[.nodes[]?|select((.node_id//.id//.name)==\"brk2\" and .phase==\"VOTER\")]|length==1' >/dev/null"

# RED: the online removal paths refuse the ghost today (the three-non deadlock). node remove needs the
# machine-escape confirm (--confirm-node-id + TETHER_CONFIRM_NODE_ID env) to get PAST the TTY-confirm and
# reach the real removal gate. Signatures are broad — they cover the captured refusals for BOTH the
# ghost-retained shape (phase-gate: "node is VOTER…") and the fully-ejected shape ("no such roster node"
# / "not in cluster_nodes"); either way the ghost cannot be cleanly removed online.
# M2: signatures pinned to CAPTURED product strings only (no success-adjacent / over-broad tokens that
# would false-green or defeat the flip-to-regression signal). Covers both observed shapes: ghost-retained
# (phase-gate "node is VOTER") and fully-ejected ("no such roster node" / "not in cluster_nodes").
assert_bug "recovery node remove brk2 --manual refuses" "#12" \
    "no such roster node|is VOTER|only.*(RETIRING|VOTER_ADD_FAILED|CATCHING_UP)" \
    "$SIM" exec brk1 -- env TETHER_CONFIRM_NODE_ID=brk2 tether cluster recovery node remove brk2 --manual --confirm-node-id brk2
assert_bug "cluster retire brk2 refuses" "#12" \
    "not in cluster_nodes|cannot retire.*last voter|cannot retire the last" \
    "$SIM" exec brk1 -- tether cluster retire brk2
assert_bug "reconcile nats --to-standalone refuses (ghost blocks proving N=1)" "#12" \
    "unrecognized raft role.*cannot prove N=1|cannot prove N=1.*unrecognized raft role" \
    "$SIM" exec brk1 -- tether cluster reconcile nats --to-standalone --confirm-single --server-name brk1

# FLIP TO PURE-REGRESSION WHEN force-single downgrades the ejected roster row / a removal path succeeds.
drill_end
