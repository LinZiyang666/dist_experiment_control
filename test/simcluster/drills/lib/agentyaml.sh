# drills/lib/agentyaml.sh — agent.yaml provisioning fixture for S1-61/62. Sourced by drills.
# Depends on lib/docker.sh (dexec, d, ctr_name), lib/log.sh (poll_until, warn), lib/tether.sh (sctl).
#
# WHY THIS EXISTS (roadmap §1.4 fidelity debt): the sim's `cmd_agent_join` writes NO agent.yaml and its
# unit's ExecStart carries `--nats-url`, which flag-shadows any yaml broker_url (`pickFlagOrYaml`,
# cmd/tether/serve.go:306). A real install.sh agent (scripts/install.sh install_agent) writes a FULL
# agent.yaml at $HOME/.tether/agent/$SESSION/agent.yaml (0600, under a 0700 agent/<sid> tree) and runs
# the daemon FLAGLESS so that yaml is fully load-bearing. This helper reproduces exactly that — the
# sim's job (Mandate ③), not tether's — so 61/62 can exercise the real on-disk policy face across a real
# restart. It is a DRILL FIXTURE, never an operator verb.
#
# Mandate-④: writes the exact file install.sh writes (0600 file, 0700 agent/<sid> tree, sim:sim) + the
# operator's documented file_transfer/remote_fs edit, and runs the agent flagless so the yaml broker_url
# is authoritative — NOT compensating for a tether gap. (S0-隧道 landed in S2 (roadmap §2.1): S1 wrote
# tunnel_addr faithfully but asserted no expose reachability; S2 FIRES that assertion — drill 81's active-
# expose arm + the T0-T6 feasibility gate curl a real body through the reverse tunnel. If a set tunnel_addr
# ever prevented the agent from reaching ONLINE, the ONLINE poll below FAILS LOUD — an exposure, not a mask.)
#
# Prereq: `cmd_agent_join <agt>` must have run first (binds the nkey via --pin first-connect into
# /home/sim/.tether). This helper then OVERWRITES agent.yaml + the unit (flagless) + restarts + polls
# ONLINE. Returns 0 iff the agent parsed the strict-KnownFields yaml and re-registered ONLINE.
#
# Usage: agent_provision_yaml <agt> <sid> <broker_url> <policy> [online_timeout_s]
#   policy = open | disabled | narrow:<abs_root> | remotefs:<auto|off> | badkey (S1-02 guard self-test)
#   online_timeout_s: how long to wait for the NEW daemon to register ONLINE (default 45; a self-test
#                     using `badkey` should pass a SHORT value since it is expected to never register).
#
# ONLINE poll (S1-02): node status is heartbeat-timestamp-driven (StaleAfter=5s), so a plain
# `restart` + "poll some agent ONLINE" would false-pass across a config-only restart even if the new
# daemon crash-looped on a bad yaml — the old row stays ONLINE for ~5s. So this does STOP → wait until
# the old row LEAVES ONLINE → START → wait until ONLINE again: the ONLINE we then see can only be the
# freshly-registered new yaml-driven daemon (the honest exposure; a bad reprovision times out + FAILS).
_apy_online()     { "$SIM" ctl -- node ls 2>/dev/null | grep -qE "^$1[[:space:]].*ONLINE"; }
_apy_not_online() { ! "$SIM" ctl -- node ls 2>/dev/null | grep -qE "^$1[[:space:]].*ONLINE"; }

agent_provision_yaml() {
    _apy_agt=$1; _apy_sid=$2; _apy_url=$3; _apy_policy=$4
    _apy_host=$(printf '%s' "$_apy_url" | sed -E 's#^nats://##; s#:[0-9]+$##')
    _apy_tunnel="$_apy_host:7000"
    _apy_root=""

    # policy → the file_transfer / remote_fs block appended under the install.sh header.
    _apy_block=""
    case "$_apy_policy" in
        open)        _apy_block="" ;;
        disabled)    _apy_block="file_transfer:
  allow_roots: []" ;;
        narrow:*)    _apy_root=${_apy_policy#narrow:}
                     _apy_block="file_transfer:
  allow_roots:
    - $_apy_root" ;;
        remotefs:*)  _apy_mode=${_apy_policy#remotefs:}
                     # quote the mode: bare `off` is a YAML 1.1 false; the field is a string wanting "off".
                     _apy_block="remote_fs:
  mode: \"$_apy_mode\"" ;;
        badkey)      # S1-02 guard self-test: an UNKNOWN key → strict KnownFields(true) rejects → the
                     # agent New() fails → the daemon crash-loops → the ONLINE gate below MUST time out +
                     # FAIL (proves the guard actually catches a bad reprovision, not a residual-ONLINE pass).
                     _apy_block="bogus_unknown_key_s1_selftest: true" ;;
        *) warn "agent_provision_yaml: unknown policy '$_apy_policy'"; return 2 ;;
    esac

    # NARROW: the allow_root dir must EXIST + be sim-owned BEFORE the restart — CanonAllowRoots resolves
    # roots once at agent New() and DROPS non-existent ones (an all-dropped narrow stays reject-all, so a
    # missing dir would make "narrowing works" vacuous). This is real operator provisioning (the data dir
    # exists at boot), not filling a tether gap.
    if [ -n "$_apy_root" ]; then
        dexec "$_apy_agt" -- install -d -o sim -g sim "$_apy_root" || { warn "agent_provision_yaml: mkdir $_apy_root failed"; return 1; }
    fi

    # write the FULL agent.yaml at the real install.sh path. Build on the host, docker cp in, then own it
    # sim:sim 0600 under the 0700 agent/<sid> tree (mirrors install.sh:315-317,346-360).
    _apy_tmp=$(mktemp) || return 1
    {
        printf 'broker_url: %s\n' "$_apy_url"
        printf 'session: %s\n'    "$_apy_sid"
        printf 'nid: %s\n'        "$_apy_agt"
        printf 'tunnel_addr: %s\n' "$_apy_tunnel"
        [ -n "$_apy_block" ] && printf '%s\n' "$_apy_block"
    } > "$_apy_tmp"
    dexec -u sim "$_apy_agt" -- env HOME=/home/sim install -d -m 0700 "/home/sim/.tether/agent/$_apy_sid" \
        || { warn "agent_provision_yaml: mkdir agent/$_apy_sid failed"; rm -f "$_apy_tmp"; return 1; }
    d cp "$_apy_tmp" "$(ctr_name "$_apy_agt"):/home/sim/.tether/agent/$_apy_sid/agent.yaml" \
        || { warn "agent_provision_yaml: docker cp agent.yaml failed"; rm -f "$_apy_tmp"; return 1; }
    rm -f "$_apy_tmp"
    dexec "$_apy_agt" -- sh -c "chown sim:sim /home/sim/.tether/agent/$_apy_sid/agent.yaml && chmod 600 /home/sim/.tether/agent/$_apy_sid/agent.yaml" \
        || { warn "agent_provision_yaml: chown/chmod agent.yaml failed"; return 1; }

    # rewrite the unit FLAGLESS (no --nats-url) so the yaml broker_url is authoritative, exactly as a real
    # install.sh agent runs. The nkey is already bound in /home/sim/.tether by the prior agent-join.
    dexec "$_apy_agt" -- sh -c "cat > /etc/systemd/system/tether-agent.service <<UNIT
[Unit]
Description=tether agent (sim, yaml-driven)
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
User=sim
Group=sim
Environment=HOME=/home/sim
ExecStart=/usr/local/bin/tether agent --session $_apy_sid --nid $_apy_agt
Restart=on-failure
RestartSec=2
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload" || { warn "agent_provision_yaml: unit rewrite failed"; return 1; }
    # STOP → wait until the old row LEAVES ONLINE → START → wait until ONLINE again (S1-02). This proves
    # the NEW yaml-driven daemon registered, not the OLD row's residual-ONLINE window.
    _apy_to=${5:-45}
    sctl "$_apy_agt" stop tether-agent || { warn "agent_provision_yaml: stop failed"; return 1; }
    poll_until 20 2 "$_apy_agt left ONLINE (old daemon stopped)" -- _apy_not_online "$_apy_agt" \
        || { warn "agent_provision_yaml: $_apy_agt never left ONLINE after stop"; return 1; }
    sctl "$_apy_agt" start tether-agent || { warn "agent_provision_yaml: start failed"; return 1; }
    # a parse error / unreachable broker_url → the new daemon never registers → this times out → FAIL.
    poll_until "$_apy_to" 3 "$_apy_agt ONLINE (new yaml-driven daemon registered, $_apy_policy)" -- _apy_online "$_apy_agt"
}
