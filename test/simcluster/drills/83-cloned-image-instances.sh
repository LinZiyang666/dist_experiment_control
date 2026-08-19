#!/bin/sh
# 83-cloned-image-instances — the deploy-tier face of the cloned-credential increment.
# Plan: docs/reviews/cloned-credential-instances-plan.md §5 T19.
# Runtime ~6min. Topology: 1 broker + 2 agents + 1 ctl (N=1 family — parallelises freely, no grow).
#
# ── WHY THIS CANNOT BE A HERMETIC TEST ──────────────────────────────────────────────────────────────
# The defect arises from copying a REAL credential and a REAL state.json into a second machine, which
# is exactly what baking an agent into a VM/container image does. A hermetic test constructs two
# agent.Config values in one process; it can model the SUBJECTS but not the on-disk artefact, the
# systemd unit, or the fact that both daemons were born from one filesystem. So this drill performs the
# actual operator action — `cp -a` the whole ~/.tether — and asserts on the product's own surfaces.
#
# ── THE CLONE IS BUILT THE WAY AN IMAGE IS BUILT (Mandate ③: the sim's job, not tether's) ────────────
# agt1 is joined normally. agt2 is NEVER joined: its identity arrives the way a clone's does, by
# copying agt1's entire /home/sim/.tether — nkey, agent.yaml, state.json, roster cache, everything —
# and then running the daemon with agt1's nid. That is not a shortcut around a tether gap; it is the
# faithful reproduction of `install.sh` + image snapshot + launch twice.
#
# ── FALSE-GREEN RISK HEADNOTE ───────────────────────────────────────────────────────────────────────
#  1. COPYING ONLY THE KEY WOULD NOT REPRODUCE THE BUG. The live evidence (plan §0.6) is that both
#     instances also share state.json — on the reference pod, literally one inode over NFS. A drill that
#     copied just the nkey would miss the inherited port tokens entirely, and the replay-gate arm below
#     would be vacuous. The copy is therefore the WHOLE tree.
#  2. `node ls` SHOWING TWO ROWS IS NOT ENOUGH. Two rows with the WRONG names (e.g. both basenames, or a
#     suffix on the incumbent) is the failure this increment exists to prevent, so B1 pins the exact
#     pair — the incumbent keeps its configured name and only the newcomer is suffixed.
#  3. ONE `exec` PRODUCING ONE RESULT IS NOT ENOUGH EITHER. ctl prints the first reply and discards the
#     rest, so a doubled execution looks identical from ctl. The oracle must be the AUDIT LOG, which
#     records every start and every exit — that is how the pre-fix defect was proved on the live fleet
#     (two start rows, two exit rows, rc=20 and rc=120 for one invocation).
#  4. R-NOSHC: never wrap a harness function in `sh -c` — the new shell has no functions, the command
#     dies as "not found", and under `!` that becomes a permanent true.
#  5. THE AUDIT COUNT MUST BE TAKEN OVER A CURSOR, not over the whole history: earlier setup traffic
#     would otherwise be counted and the arm would pass or fail for unrelated reasons.
#
# ── EXPECTED LANDING ────────────────────────────────────────────────────────────────────────────────
# GREEN on the increment. Recorded PRODUCT-RED against pre-increment code (a drill that has only ever
# been seen green proves nothing) — see expected-verdicts-log.md.

set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/agentyaml.sh"
. "$HERE/drills/lib/logs.sh"

SID=lab
PIN=838383
NURL="nats://brk1:4222"
BAKED=agt1              # the nid baked into the "image"
LEASE=agt1-02           # what the second instance must be assigned

# _clone_home reproduces an image bake: agt2 discards whatever identity it had and takes agt1's ENTIRE
# ~/.tether. Streamed as a tar through the host so ownership and modes survive, which matters because
# the agent refuses a world-readable key tree.
_clone_home() {
    sctl agt2 stop tether-agent >/dev/null 2>&1 || true
    dexec agt2 -- rm -rf /home/sim/.tether || return 1
    d exec "$(ctr_name agt1)" tar -C /home/sim -cf - .tether \
        | d exec -i "$(ctr_name agt2)" tar -C /home/sim -xf - || return 1
    dexec agt2 -- chown -R sim:sim /home/sim/.tether || return 1
}

# _run_as_baked rewrites agt2's unit to run under the BAKED nid — the whole point: two daemons, one
# configured identity, one credential.
_run_as_baked() {
    dexec agt2 -- sh -c "cat > /etc/systemd/system/tether-agent.service <<UNIT
[Unit]
Description=tether agent (sim, cloned image)
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
User=sim
Group=sim
Environment=HOME=/home/sim
ExecStart=/home/sim/.local/bin/tether agent --session $SID --nid $BAKED
Restart=on-failure
RestartSec=2
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload" || return 1
    sctl agt2 start tether-agent
}

_node_ls()        { "$SIM" ctl -- node ls 2>/dev/null; }
_row_online()     { _node_ls | grep -qE "^$1[[:space:]].*ONLINE"; }
_two_rows_online() { _row_online "$BAKED" && _row_online "$LEASE"; }
_incumbent_unsuffixed() { _node_ls | grep -qE "^$BAKED[[:space:]]"; }

# _audit_since counts audit proc rows of one kind for one node, from a cursor line count.
_audit_lines() { "$SIM" ctl -- history --kind proc -n 400 2>/dev/null; }
_audit_count_after() {
    _ac_cursor=$1; _ac_pat=$2
    _audit_lines | tail -n +"$((_ac_cursor + 1))" | grep -cE "$_ac_pat" || true
}

drill_begin "83-cloned-image-instances (one baked image, two live instances: lease adjudication + single execution)"

assert_setup "up 1 broker + 2 agents + 1 ctl"        "$SIM" up --brokers 1 --agents 2 --ctl 1
assert_setup "init brk1 (standalone -> N=1 cluster)" "$SIM" init brk1
assert_setup "session $SID + ctl login"              "$SIM" session "$SID" --pin "$PIN"
assert_setup "agent-join agt1 (the machine the image is baked FROM)" \
    "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_setup "provision agt1 agent.yaml (real install.sh shape, flagless daemon)" \
    agent_provision_yaml agt1 "$SID" "$NURL" open
assert_setup "agt1 ONLINE before the clone is introduced" \
    poll_until 30 2 "agt1 ONLINE" -- _row_online "$BAKED"

# ── the bake ────────────────────────────────────────────────────────────────────────────────────────
assert_setup "clone agt1's ENTIRE ~/.tether onto agt2 (nkey + agent.yaml + state.json + roster cache)" \
    _clone_home
assert_setup "run agt2's daemon under agt1's baked nid" _run_as_baked

# ── B1: the operator sees TWO devices, and the incumbent keeps its name ──────────────────────────────
assert_ok "B1 both instances are ONLINE as two distinct rows: the incumbent keeps '$BAKED', the clone is leased '$LEASE'" \
    poll_until 60 3 "two rows online" -- _two_rows_online
assert_ok "B1b the incumbent's row is NOT suffixed (a rename of the existing device would be the worse failure)" \
    _incumbent_unsuffixed

# ── B2: one command executes exactly once ───────────────────────────────────────────────────────────
# This is the arm that pins the live-fleet defect: before the increment, ONE exec produced TWO start
# and TWO exit audit rows because both clones received the same forwarded message.
_B2_CURSOR=$(_audit_lines | wc -l)
assert_ok "B2a exec a marker command on the baked name" \
    "$SIM" ctl -- exec "$BAKED" -- echo drill83-marker
sleep 3
assert_ok "B2b exactly ONE start row for that command (two would mean both instances ran it)" \
    out_matches '^1$' _audit_count_after "$_B2_CURSOR" 'kind=start.*drill83-marker'
assert_ok "B2c exactly ONE exit row for that command" \
    out_matches '^1$' _audit_count_after "$_B2_CURSOR" 'kind=exit'

# ── B3: the leased instance is separately addressable (I2) ──────────────────────────────────────────
assert_ok "B3 the leased instance can be targeted by name, like any other device" \
    "$SIM" ctl -- exec "$LEASE" -- echo drill83-lease-reachable

# ── B4: neither instance destroys the other's process rows ──────────────────────────────────────────
# Pre-increment, the clone's register walked the incumbent's RUNNING rows, found them absent from its
# own snapshot, and closed them all with reconciled_closed — after which the incumbent's next register
# saw them as orphans and killed them. The contested-register short-circuit is what prevents it.
assert_ok "B4 no reconciled_closed row was produced by the clone's arrival" \
    out_matches '^0$' _audit_count_after "$_B2_CURSOR" 'kind=reconciled_closed'
assert_ok "B4b no killed_orphan row either" \
    out_matches '^0$' _audit_count_after "$_B2_CURSOR" 'kind=killed_orphan'

drill_end
