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

# admit_creator <broker> <node> <user> [ENV=VAL ...] : the DOCUMENTED first-deployment step.
#
# A FRESH broker's session_creators table is EMPTY, and `session create` is refused (exit 77,
# code not_allowed) until an operator admits the caller's fingerprint ON THE BROKER HOST.
# See docs/broker-ops.md §5.20 "全新 broker 的第一步". This mirrors the real two-command
# operator procedure and adds nothing to it: the user reads their own fingerprint with
# `tether whoami`, the operator pastes it into `tether admin session-allow` over the
# root-only admin socket. It is auth setup, not a lifecycle workaround.
#
# THE IDENTITY IS THE (user, home) PAIR, NOT THE CONTAINER. cli.DefaultHome() honours
# $TETHER_HOME and EnsureIdentity mints a SEPARATE nkey per home, so ctl1's `sim` user has a
# different fingerprint for every CTLH tag. Pass the SAME env assignments the later
# `session create` will run under — admit the wrong home and you have admitted a fingerprint
# nobody uses while the create is still refused, which reads exactly like the bug this fixes.
#
# `tether whoami` MINTS the identity if it does not exist yet, which is what makes this
# usable before a probe's isolated $HOME has ever run anything.
#
# origin: prerelease audit increment 2, first simcluster sweep after the session-admission
# increment — 43/43 drills red on one root cause (an empty table on every fresh broker).
admit_creator() {
    _ad_brk=$1; _ad_node=$2; _ad_user=$3; shift 3
    _ad_fp=$(dexec -u "$_ad_user" "$_ad_node" -- env "$@" tether whoami --json 2>/dev/null \
        | jq -r '.fingerprint // empty' 2>/dev/null) || _ad_fp=""
    [ -n "$_ad_fp" ] || { err "admit_creator: no fingerprint from ${_ad_user}@${_ad_node} [$*]"; return 1; }
    # CHECK BEFORE WRITING, because a redundant admission is NOT free. `--list` is a local DB read
    # (adminsock OpSessionCreators -> session.ListCreators), but `session-allow` is a REPLICATED write
    # gated by assertAllVotersSupportSessionCreatorOps, which refuses whenever any voter is
    # unreachable. Drills legitimately re-enter this helper while the cluster is degraded on purpose —
    # 90-alerts-lifecycle calls `$SIM session` again for its N=2 arm with a broker deliberately down —
    # and there the no-op re-admit would be the ONLY thing that failed. Skipping the write is also
    # what a real operator does: you do not re-admit somebody who is already on the list.
    if dexec "$_ad_brk" -- tether admin session-allow --list 2>/dev/null | grep -qF "$_ad_fp"; then
        log "admit: ${_ad_user}@${_ad_node} [$*] → $_ad_fp already admitted on $_ad_brk (no write)"
        return 0
    fi
    dexec "$_ad_brk" -- tether admin session-allow "$_ad_fp" --note "simcluster ${_ad_user}@${_ad_node}" >/dev/null \
        || { err "admit_creator: session-allow $_ad_fp on $_ad_brk failed"; return 1; }
    log "admit: ${_ad_user}@${_ad_node} [$*] → $_ad_fp may create sessions (via $_ad_brk)"
}

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
