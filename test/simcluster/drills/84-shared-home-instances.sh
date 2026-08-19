#!/usr/bin/env bash
# 84-shared-home-instances — TWO INSTANCES ON ONE INODE.
#
# WHY THIS EXISTS SEPARATELY FROM 83
#
# Drill 83 bakes an image the honest way for a container fleet: it streams agt1's entire ~/.tether onto
# agt2, so both hold the same credential. What it does NOT reproduce is the reference deployment's
# actual shape — on the live fleet ~/.tether is an NFS mount, so the instances of one cloned image are
# not looking at copies of state.json and the upgrade marker, they are looking at ONE FILE. Every
# interesting failure in that deployment lives in the sharing, not in the credential:
#
#   - state.json: a leased instance must not write it (it belongs to whoever holds the basename), and
#     the basename holder's exposes must survive its neighbour existing;
#   - the upgrade marker: it sits next to a SHARED binary, so ownership cannot be established from the
#     binary path or its SHA — external review F7 is exactly this, and its residual limitation is only
#     observable here;
#   - the agent key tree: two processes opening the same key files.
#
# external review F14 called the gap out: 83 was registered GREEN while the highest-risk deployment
# form had no deploy-tier oracle at all. This drill is that oracle.
#
# HOW THE SHARING IS BUILT
#
# `up --shared-agent-home` mounts ONE named docker volume as /home/sim on every agent
# container, which is genuinely one inode per file — not a copy, not a bind of two directories. The
# sim owns that (Mandate ③: reproduce the deployment, do not compensate for the product).
#
# ── EXPECTED LANDING ────────────────────────────────────────────────────────────────────────────────
# GREEN on the increment. The B-arm assertions are the ones that would go red if a leased instance
# ever wrote the shared state.json, or if the two instances collapsed back into one name.

set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/agentyaml.sh"
. "$HERE/drills/lib/logs.sh"

SID=lab
PIN=848484
NURL="nats://brk1:4222"
BAKED=agt1              # the nid both instances are configured with
LEASE=agt1-02           # what the second instance must be assigned

_node_ls() { "$SIM" ctl -- node ls -a 2>/dev/null; }
_row_online() { _node_ls | grep -qE "^$1[[:space:]].*ONLINE"; }
_two_rows_online() { _row_online "$BAKED" && _row_online "$LEASE"; }
_incumbent_unsuffixed() { _node_ls | grep -qE "^$BAKED[[:space:]]"; }

# _same_inode proves the sharing is real rather than assumed. If this fails the rest of the drill is
# measuring drill 83 again, so it is a SETUP assertion: a false green here would be the worst outcome.
_same_inode() {
    _si_home_a=$(dexec agt1 -- stat -c %i /home/sim/.tether 2>/dev/null)
    _si_home_b=$(dexec agt2 -- stat -c %i /home/sim/.tether 2>/dev/null)
    _si_bin_a=$(dexec agt1 -- stat -c '%d:%i' /home/sim/.local/bin/tether 2>/dev/null)
    _si_bin_b=$(dexec agt2 -- stat -c '%d:%i' /home/sim/.local/bin/tether 2>/dev/null)
    [ -n "$_si_home_a" ] && [ "$_si_home_a" = "$_si_home_b" ] &&
        [ -n "$_si_bin_a" ] && [ "$_si_bin_a" = "$_si_bin_b" ]
}

# _run_as_baked points agt2's unit at the SAME nid agt1 uses. No credential copying is needed here —
# the home, and therefore the key tree, is already shared.
_run_as_baked() {
    dexec agt2 -- sh -c "cat > /etc/systemd/system/tether-agent.service <<UNIT
[Unit]
Description=tether agent (sim, shared home)
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
systemctl daemon-reload && systemctl enable --now tether-agent" >/dev/null 2>&1
}

_state_json() { dexec agt1 -- cat "/home/sim/.tether/agent/$SID/state.json" 2>/dev/null; }
_port_token_count() { _state_json | grep -o '"port"' | wc -l | tr -d ' '; }

drill_begin "84-shared-home-instances (one NFS-shaped home, two live instances: shared state + marker)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_setup "up 1 broker + 2 agents + 1 ctl, agents sharing ONE home volume" \
    "$SIM" up --brokers 1 --agents 2 --ctl 1 --shared-agent-home
assert_setup "init brk1 (standalone -> N=1 cluster)" "$SIM" init brk1
assert_setup "session $SID + ctl login"              "$SIM" session "$SID" --pin "$PIN"

# THE SHARING ITSELF, asserted before anything depends on it.
assert_setup "both agents see the SAME .tether directory AND tether binary inode (not copies)" _same_inode

assert_setup "agent-join agt1 (writes the credential into the shared home)" \
    "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_setup "provision agt1 agent.yaml (real install.sh shape, flagless daemon)" \
    agent_provision_yaml agt1 "$SID" "$NURL" open
assert_setup "agt1 ONLINE before the second instance starts" \
    poll_until 30 2 "agt1 ONLINE" -- _row_online "$BAKED"

# S1: the incumbent owns the shared state file. Expose a port so there is something to protect.
assert_ok "S1 incumbent can expose (its ports live in the shared state.json)" \
    "$SIM" ctl -- expose "$BAKED" --local 18080 --name shared-web
sleep 2
_S1_TOKENS=$(_port_token_count)
assert_ok "S1b the incumbent's port token is persisted in the shared state.json" \
    test "$_S1_TOKENS" -ge 1

# ── the second instance, on the same home ───────────────────────────────────────────────────────────
assert_setup "start agt2's daemon under agt1's nid, on the SHARED home" _run_as_baked

assert_ok "S2 both instances are ONLINE as two distinct rows ('$BAKED' + leased '$LEASE')" \
    poll_until 60 3 "two rows online" -- _two_rows_online
assert_ok "S2b the incumbent's row is NOT suffixed" \
    _incumbent_unsuffixed

# S3: THE SHARED-STATE INVARIANT. The leased instance must not have written the file that belongs to
# the basename holder. If it had, the incumbent's token count would have been overwritten by a
# snapshot that does not include its port.
sleep 3
assert_ok "S3 the incumbent's port token SURVIVED the leased instance's arrival (it must not write the shared state.json)" \
    test "$(_port_token_count)" -ge "$_S1_TOKENS"

# S4: one command, one execution — the same oracle as drill 83, but with the home shared.
_audit_lines() { "$SIM" ctl -- history --kind proc -n 400 2>/dev/null; }
_audit_count_after() {
    _ac_cursor=$1; _ac_pat=$2
    _audit_lines | tail -n +"$((_ac_cursor + 1))" | grep -cE "$_ac_pat" || true
}
_S4_CURSOR=$(_audit_lines | wc -l)
assert_ok "S4 exec a marker command on the baked name" \
    "$SIM" ctl -- exec "$BAKED" -- echo drill84-marker
sleep 3
assert_ok "S4b exactly ONE start row (two would mean both instances ran it)" \
    out_matches '^1$' _audit_count_after "$_S4_CURSOR" 'kind=start.*drill84-marker'
assert_ok "S4c exactly ONE exit row" \
    out_matches '^1$' _audit_count_after "$_S4_CURSOR" 'kind=exit'

# S5: the leased instance is separately addressable (I2), on a shared home too.
assert_ok "S5 the leased instance can be targeted by name" \
    "$SIM" ctl -- exec "$LEASE" -- echo drill84-lease-reachable

# S6: there is no honest per-instance upgrade boundary on a shared binary. Both
# the leased row and its basename are therefore refused BEFORE any download.
_ZERO_SHA=0000000000000000000000000000000000000000000000000000000000000000
_UP_URL=https://github.com/LinZiyang666/dist_experiment_control/releases/download/v0.0.0/tether.tar.gz
assert_refuses "S6 leased instance remote upgrade is refused on the shared binary" \
    "clone_family_upgrade_unsupported" \
    "$SIM" ctl -- node upgrade "$LEASE" --wait=false --url "$_UP_URL" --sha256 "$_ZERO_SHA"
assert_refuses "S6b basename remote upgrade is also refused while the clone family exists" \
    "clone_family_upgrade_unsupported" \
    "$SIM" ctl -- node upgrade "$BAKED" --wait=false --url "$_UP_URL" --sha256 "$_ZERO_SHA"

drill_end
