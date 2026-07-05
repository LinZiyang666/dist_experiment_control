# lib/tether.sh — tether CLI + cluster helpers inside sim node containers. Sourced by simcluster.
# Depends on lib/docker.sh (dexec, list_nodes, node_running) + lib/log.sh (poll_until).

# tctl <node> [-u USER] -- <tether args...>
# Runs the real tether CLI inside a node. Default user = tether (matches install.sh User=tether,
# dodges the root-owned tether.lock gotcha #6). Admin-socket discovery: rely on broker.yaml's
# admin.socket default first; if a call can't reach the socket, wire --config/--socket here.
tctl() {
    _tc_u=tether
    _tc_n=$1; shift
    if [ "${1:-}" = "-u" ]; then _tc_u=$2; shift 2; fi
    [ "${1:-}" = "--" ] && shift
    dexec -u "$_tc_u" "$_tc_n" -- tether "$@"
}

# sctl <node> <systemctl args...> : systemctl inside a node container.
sctl() { _sc_n=$1; shift; dexec "$_sc_n" -- systemctl "$@"; }

cluster_status_json() { tctl "$1" -- cluster status --json 2>/dev/null; }

# leader_node : node_id of the current raft leader (server_name == node_id). Query any running broker.
leader_node() {
    for _ln_b in $(list_nodes broker); do
        node_running "$_ln_b" || continue
        _ln_lid=$(cluster_status_json "$_ln_b" | jq -r '.leader_id // empty' 2>/dev/null || true)
        [ -n "${_ln_lid:-}" ] && { printf '%s' "$_ln_lid"; return 0; }
    done
    return 1
}

# _phase_is <node> <PHASE> : predicate — the node's cluster phase (via the leader's view) == PHASE.
# reachable/applied_lag/phase are leader-only-populated (adminsock/protocol.go:415), so evaluate
# against the leader. Per-node id key tolerated (.node_id // .id // .name) pending real-output verify.
_phase_is() {
    _pi_leader=$(leader_node) || return 1
    _pi_got=$(cluster_status_json "$_pi_leader" \
        | jq -r --arg n "$1" '.nodes[]? | select((.node_id // .id // .name)==$n) | .phase' 2>/dev/null || true)
    [ "${_pi_got:-}" = "$2" ]
}

# wait_phase <node> <PHASE> [timeout_s] : poll until the node reaches PHASE (no fixed sleeps).
wait_phase() { poll_until "${3:-300}" 3 "$1 phase=$2" -- _phase_is "$1" "$2"; }

# _health_is <expected...> : predicate — leader's top-level health ∈ the given set.
_health_is() {
    _hi_leader=$(leader_node) || return 1
    _hi_got=$(cluster_status_json "$_hi_leader" | jq -r '.health // empty' 2>/dev/null || true)
    for _hi_want in "$@"; do [ "${_hi_got:-}" = "$_hi_want" ] && return 0; done
    return 1
}
