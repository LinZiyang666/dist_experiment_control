# lib/docker.sh — docker orchestration helpers. POSIX sh. Sourced by simcluster.
# Addressing model: user-defined bridge per instance; nodes are reached by HOSTNAME (== node_id)
# via docker's embedded DNS, so raft/route/tunnel addresses (brk1:7400 / :6222 / :7000) are stable
# across restart without static-IP subnet bookkeeping, and cert SANs key on DNS:<node_id>.

: "${INSTANCE:=sim}"
: "${IMAGE:=tether-sim:dev}"
: "${DOCKER:=docker}"          # set DOCKER="sudo -n docker" or DOCKER_SUDO=1 on a locked-down host

d() {  # docker wrapper (honors DOCKER_SUDO for hosts where weiland is not in the docker group)
    if [ "${DOCKER_SUDO:-0}" = "1" ]; then sudo docker "$@"; else $DOCKER "$@"; fi
}

net_name()  { printf 'sim-%s' "$INSTANCE"; }
ctr_name()  { printf 'sim-%s-%s' "$INSTANCE" "$1"; }
vol_etc()   { printf 'sim-%s-%s-etc' "$INSTANCE" "$1"; }
vol_lib()   { printf 'sim-%s-%s-lib' "$INSTANCE" "$1"; }

ensure_net() {
    assert_host_dns_says_no
    d network inspect "$(net_name)" >/dev/null 2>&1 && return 0
    run d network create --driver bridge "$(net_name)" >/dev/null
}

# assert_host_dns_says_no — refuse to run when the host resolver fabricates an address for names that
# do not exist.
#
# origin: line-2 external review follow-up. Drill 42 produced ASSERT-FAIL twice with a contradiction
# nobody could explain: the sim proved brk2:7400 was connection-refused, and moments later the product
# reported that brk2:7400 had ACCEPTED a TCP connection. Measured from inside brk1 after killing brk2:
#
#     getent hosts brk2                          ->  198.18.0.58   brk2.lan
#     getent hosts thisnamedoesnotexist12345     ->  198.18.0.59   thisnamedoesnotexist12345.lan
#
# 198.18.0.0/15 is mihomo/clash FAKE-IP. This host runs mihomo with `search lan`, so once brk2's
# container is gone and docker's embedded DNS stops knowing the name, the query is forwarded upstream
# and comes back with a synthetic address whose TUN device COMPLETES the TCP handshake. Every
# TCP-based liveness probe on such a host reports ALIVE for a machine that does not exist.
#
# WHY REFUSE RATHER THAN WORK AROUND IT. The Mandate (README §"定位铁律") is to reproduce the real
# deployment faithfully and expose defects, never to compensate for tether. Pinning container IPs or
# probing by address would make drill 42 green while leaving the drill unable to measure the thing it
# exists to measure — a green that means nothing. Refusing says what is wrong and what to do about it.
#
# This is NOT a tether defect and NOT a drill defect: it is the host. But see the note in the external
# review reply — tether's own probePeer has the same blind spot in production, where the consequence is
# an operator permanently blocked from a legitimate force-single.
assert_host_dns_says_no() {
    [ "${SIM_ALLOW_FAKE_DNS:-0}" = "1" ] && return 0
    _adns_probe="sim-nxdomain-probe-$$-does-not-exist"
    # NXDOMAIN is getent rc=2. simcluster runs with `set -euo pipefail`, so
    # without the explicit fallback the assignment itself terminates the whole
    # command before the empty-output success branch below can run.
    _adns_out=$(getent hosts "$_adns_probe" 2>/dev/null | head -1 || true)
    [ -z "$_adns_out" ] && return 0
    printf 'SIM-PREFLIGHT-FAIL: this host resolves names that do not exist.\n' >&2
    printf '  getent hosts %s  ->  %s\n' "$_adns_probe" "$_adns_out" >&2
    printf '\n' >&2
    printf '  Every drill that proves a peer is DEAD by TCP-probing it is unmeasurable here: the\n' >&2
    printf '  resolver hands back a synthetic address (mihomo/clash fake-IP is 198.18.0.0/15) whose\n' >&2
    printf '  TUN device completes the handshake, so a killed node reads as ALIVE. Drill 42 spent two\n' >&2
    printf '  full runs producing ASSERT-FAIL from exactly this.\n' >&2
    printf '\n' >&2
    printf '  Fix the host, do not work around it:\n' >&2
    printf '    - stop the fake-IP resolver (mihomo/clash) for the duration of the drill, or\n' >&2
    printf '    - switch it to redir-host mode, or\n' >&2
    printf '    - point /etc/resolv.conf at a resolver that returns NXDOMAIN.\n' >&2
    printf '  SIM_ALLOW_FAKE_DNS=1 overrides this check. Do not use it for any drill that asserts a\n' >&2
    printf '  node is dead — the result would be a green that measured nothing.\n' >&2
    return 1
}

node_exists()  { d inspect "$(ctr_name "$1")" >/dev/null 2>&1; }
node_running() { [ "$(d inspect -f '{{.State.Running}}' "$(ctr_name "$1")" 2>/dev/null)" = "true" ]; }

# run_node <role> <node> [extra docker run flags...]
# Boots bare systemd (PID1) + sshd. Provisioning + unit start are driven later by the control script.
run_node() {
    _rn_role=$1; _rn_node=$2; shift 2
    # Internal orchestration flag: replace (rather than stack on top of) the
    # normal /var/lib/tether named volume. Docker rejects duplicate mount
    # destinations, and mounting only the jetstream child breaks the product's
    # atomic move-aside during grow with EBUSY.
    _rn_lib_mount=""
    if [ "${1:-}" = "--sim-lib-tmpfs" ]; then
        _rn_lib_mount=$2
        shift 2
    fi
    ensure_net
    if [ -n "$_rn_lib_mount" ]; then
        set -- --tmpfs "/var/lib/tether:size=$_rn_lib_mount" "$@"
    else
        set -- -v "$(vol_lib "$_rn_node"):/var/lib/tether" "$@"
    fi
    run d run -d \
        --name "$(ctr_name "$_rn_node")" --hostname "$_rn_node" \
        --network "$(net_name)" --network-alias "$_rn_node" \
        --privileged --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
        --tmpfs /run --tmpfs /run/lock --stop-signal SIGRTMIN+3 \
        --restart no \
        --label sim.instance="$INSTANCE" --label sim.role="$_rn_role" --label sim.nodeid="$_rn_node" \
        -v "$(vol_etc "$_rn_node"):/etc/tether" \
        "$@" \
        "$IMAGE" >/dev/null
}

# dexec [-u USER] <node> -- <cmd...>
dexec() {
    _de_user=""
    if [ "$1" = "-u" ]; then _de_user="-u $2"; shift 2; fi
    _de_node=$1; shift
    [ "$1" = "--" ] && shift
    # shellcheck disable=SC2086
    d exec $_de_user "$(ctr_name "$_de_node")" "$@"
}

# dexec_it <node> -- <cmd...>  (interactive, for `shell`)
dexec_it() {
    _dei_node=$1; shift
    [ "$1" = "--" ] && shift
    d exec -it "$(ctr_name "$_dei_node")" "$@"
}

# list nodes of this instance (optionally filtered by role): prints node ids.
# NB: each --filter must be its OWN flag+value pair; embedding "--filter …" inside a single
# filter string makes docker treat it as one label value and match nothing (first-run bug).
list_nodes() {
    if [ -n "${1:-}" ]; then
        d ps -a --filter "label=sim.instance=$INSTANCE" --filter "label=sim.role=$1" --format '{{.Label "sim.nodeid"}}' | sort
    else
        d ps -a --filter "label=sim.instance=$INSTANCE" --format '{{.Label "sim.nodeid"}}' | sort
    fi
}

# tcp_refused <node> <port>: succeeds when a TCP connect to node:port is REFUSED (peer dead).
# Used by force-single to poll-until-ports-refused instead of a fixed sleep.
tcp_refused() {
    ! d run --rm --network "$(net_name)" "$IMAGE" \
        bash -c "timeout 2 bash -c '</dev/tcp/$1/$2' 2>/dev/null"
}

rm_node() {  # rm_node <node> [--vols]
    _rmn_node=$1
    run d rm -f "$(ctr_name "$_rmn_node")" >/dev/null 2>&1 || true
    if [ "${2:-}" = "--vols" ]; then
        d volume rm "$(vol_etc "$_rmn_node")" "$(vol_lib "$_rmn_node")" >/dev/null 2>&1 || true
    fi
}

# node_kill / node_stop / node_start: crash/return primitives that KEEP the container + its named
# volumes (persistent raft/etc/lib state), so the SAME node can rejoin via node_start. Distinct from
# rm_node, which destroys container+vols (a node that can NEVER return). Used by the failover/rehome
# arms where a broker must come BACK and catch up to VOTER (71-C/D return, 73-REHOME, 74-return).
# Only the "killed forever" arms (73 quorum-loss victims) use rm_node. Poll death with tcp_refused.
#   node_kill  <node> [signal] : hard SIGKILL of PID1 (power-loss class; default KILL).
#   node_stop  <node>          : graceful stop (--stop-signal SIGRTMIN+3 → systemd clean shutdown).
#   node_start <node>          : bring a killed/stopped node back (same vols; --restart no means it
#                                stays down until this). Provisioning persists (sentinel-guarded).
node_kill()  { run d kill ${2:+--signal "$2"} "$(ctr_name "$1")" >/dev/null; }
node_stop()  { run d stop "$(ctr_name "$1")" >/dev/null; }
node_start() { run d start "$(ctr_name "$1")" >/dev/null; }
