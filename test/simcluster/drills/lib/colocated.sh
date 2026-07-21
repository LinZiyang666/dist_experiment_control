# drills/lib/colocated.sh — OQ-6 SUPPLY: a tether AGENT co-located on a BROKER host.
#
# WHY THIS IS PROVISIONING (Mandate ③), NOT THE SIM DOING TETHER'S JOB
# --------------------------------------------------------------------
# `serve --colocated-agent-nid` / broker.yaml `broker.cluster.colocated_agent_nid` (serveconf.go:76-80) is a
# first-class, documented deployment shape: a broker host that ALSO runs an agent. For such a host, "upgraded"
# means BOTH daemons reached the target — that is the single #19 whole-host criterion (`whole_host_at`,
# node_versions.go:35) and the reason cluster_upgrade_drive.go:104-121 has an agent leg at all.
#
# The sim's brokers never ran one. So drill 30's whole-host leg was STRUCTURALLY unreachable and had been
# standing as a permanent `not_covered "(a) colocated-agent whole-host leg — OQ-6"` since the drill was
# written. The 2026-07-18 full-suite review adjudicated OQ-6 as a SUPPLY gap, not a tether defect: `serve`'s
# flag defaults empty and an agentless broker host is a legitimate production shape, so tether is not wrong —
# the sim simply never built the machine. Building that machine is exactly what Mandate ③ assigns to the sim
# (install.sh, secrets, containers, daemons). Everything the UPGRADE then does is still 100% tether's:
# `cluster upgrade` plans the host, re-execs the broker, re-execs the agent, and renders the verdict. This
# file starts a daemon; it never upgrades, re-execs, or version-fixes anything.
#
# THE BINARY PATH IS DELIBERATELY THE ROOT-OWNED /usr/local/bin/tether
# --------------------------------------------------------------------
# The standalone `agent` role installs to ~/.local/bin because `node upgrade` DOWNLOADS and atomically
# replaces the binary, which needs write permission on the parent directory (image/provision-node.sh
# documents exactly this, and explicitly excludes the co-located case). `cluster upgrade`'s agent leg is a
# different mechanism: REEXEC-ONLY (agent/upgrade.go handleReExecOnly) — the operator has already staged the
# shared binary as a privileged step, and the agent only sha-verifies and syscall.Exec's it, which needs no
# directory write at all. Pointing the co-located agent at a private copy would make its version advance for a
# reason the real deployment does not have, and would silently stop testing the staged-binary path.
#
# WHAT THE THREE PRESENCE STATES NEED FROM THIS FILE (P3, R9 product side)
# ------------------------------------------------------------------------
#   DECLARED  — `colocated_agent_declare` writes the nid into broker.yaml. Presence then holds even when the
#               agent is DOWN (cluster_upgrade.go:331), so the roll HALTs loudly instead of skipping the host.
#               Use a nid DIFFERENT from the node_id, or the declaration is indistinguishable from the
#               node_id==nid convention and `assumed` stays true.
#   OBSERVED  — start the agent with nid == node_id and do NOT declare. Presence is then inferred from the
#               session node list (`assumed: true` in `node ls --brokers`).
#   ABSENT    — do nothing at all for that host. No agent ever registered under its node_id ⇒ state (c),
#               broker-only.
#
# Depends on lib/docker.sh (dexec), lib/tether.sh (sctl), lib/log.sh (log/poll_until) + the drill's $SIM.

_COLO_SIM="${SIM:-$HERE/simcluster}"

# colocated_agent_declare <broker> <nid> : declare <nid> as this broker's co-located agent in the ROOT-owned
# /etc/tether/broker.yaml, then restart the broker so `serve` reads it. Idempotent; verifies the key landed
# (a silently-unwritten key would make every later "declared" assertion measure the OBSERVED path instead).
colocated_agent_declare() {
    _cad_b=$1; _cad_nid=$2
    dexec "$_cad_b" -- sh -c "grep -qE '^[[:space:]]*colocated_agent_nid:' /etc/tether/broker.yaml || sed -i -E '/^  cluster:/a\\    colocated_agent_nid: $_cad_nid' /etc/tether/broker.yaml" || return 1
    dexec "$_cad_b" -- sh -c "grep -qE '^[[:space:]]*colocated_agent_nid:[[:space:]]*$_cad_nid[[:space:]]*\$' /etc/tether/broker.yaml" || {
        warn "colocated_agent_declare: colocated_agent_nid=$_cad_nid did NOT land in $_cad_b's broker.yaml"
        return 1
    }
    sctl "$_cad_b" restart tether-broker >/dev/null 2>&1 || return 1
    poll_until 90 3 "$_cad_b broker back up after declaring colocated agent $_cad_nid" -- _colo_broker_ready "$_cad_b"
}

_colo_broker_ready() { dexec -u tether "$1" -- tether cluster status --json >/dev/null 2>&1; }

# _colo_online <nid> : the agent is ONLINE in the ctl's session node list.
_colo_online() { "$_COLO_SIM" ctl -- node ls 2>/dev/null | grep -qE "^$1[[:space:]].*ONLINE"; }

# colocated_agent_up <broker> <sid> <pin> <nid> : bind <nid> into the session (the `--pin` first-connect, the
# same first-class onboarding path cmd_agent_join uses) and run it as a persistent system unit on the BROKER
# host, out of the staged /usr/local/bin/tether. Returns non-zero unless it reaches ONLINE.
colocated_agent_up() {
    _cau_b=$1; _cau_sid=$2; _cau_pin=$3; _cau_nid=$4
    _cau_url="nats://$_cau_b:4222"
    log "colocated: bind + start agent nid=$_cau_nid ON broker host $_cau_b (binary=/usr/local/bin/tether, user=sim)"
    # $HOME for the agent identity. NOT /var/lib/tether: that home already holds the broker-local ctl login
    # the drills perform as the tether uid, and cli.EnsureIdentity is per-HOME — sharing it would make the
    # agent and the ctl the SAME nkey and collide their session roles.
    dexec "$_cau_b" -- sh -c 'id -u sim >/dev/null 2>&1 || useradd -m -s /bin/bash sim; install -o sim -g sim -m 0755 -d /home/sim/.tether' || return 1
    # first --pin connect binds the agent nkey as a session member and persists it under /home/sim/.tether.
    dexec -u sim "$_cau_b" -- sh -c "HOME=/home/sim timeout 8 tether agent --session '$_cau_sid' --nid '$_cau_nid' --pin '$_cau_pin' --nats-url '$_cau_url' >/tmp/colo-bind-$_cau_nid.log 2>&1 || true"
    dexec "$_cau_b" -- sh -c "printf 'SID=%s\nNID=%s\nNATS_URL=%s\n' '$_cau_sid' '$_cau_nid' '$_cau_url' > /etc/tether/colo-agent.env
cat > /etc/systemd/system/tether-agent.service <<UNIT
[Unit]
Description=tether agent co-located on a broker host (sim, OQ-6)
After=network-online.target tether-broker.service
[Service]
Type=simple
User=sim
Environment=HOME=/home/sim
EnvironmentFile=/etc/tether/colo-agent.env
ExecStart=/usr/local/bin/tether agent --session \\\${SID} --nid \\\${NID} --nats-url \\\${NATS_URL}
Restart=on-failure
RestartSec=2
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload" || return 1
    sctl "$_cau_b" enable --now tether-agent >/dev/null 2>&1 || return 1
    # Type=simple returns the instant the process is exec'd, BEFORE it registers — verify by RESULT.
    poll_until 60 3 "colocated agent $_cau_nid on $_cau_b is ONLINE in the session roster" -- _colo_online "$_cau_nid"
}

# colocated_agent_stop / _start <broker> : take the co-located agent down / bring it back. Used to construct
# P3's "DECLARED but currently DOWN" host, which must still count as PRESENT (⇒ a loud HALT), never as absent.
colocated_agent_stop()  { sctl "$1" stop tether-agent >/dev/null 2>&1; }
colocated_agent_start() { sctl "$1" start tether-agent >/dev/null 2>&1; }

# colo_agent_ver <nid> : the release `node ls --brokers --json` correlates for this agent nid, read from the
# BROKER row that owns it — i.e. exactly the field `cluster upgrade`'s whole-host verdict is built on, not a
# separate lookup that could agree with a broken correlation by accident.
colo_agent_ver() {
    "$_COLO_SIM" ctl -- node ls --brokers --json 2>/dev/null \
        | jq -r --arg n "$1" '.brokers[]?|select(.agent_nid==$n)|.agent_ver // empty' 2>/dev/null
}
