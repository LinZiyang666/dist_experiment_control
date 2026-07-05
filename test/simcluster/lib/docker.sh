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
    d network inspect "$(net_name)" >/dev/null 2>&1 && return 0
    run d network create --driver bridge "$(net_name)" >/dev/null
}

node_exists()  { d inspect "$(ctr_name "$1")" >/dev/null 2>&1; }
node_running() { [ "$(d inspect -f '{{.State.Running}}' "$(ctr_name "$1")" 2>/dev/null)" = "true" ]; }

# run_node <role> <node> [extra docker run flags...]
# Boots bare systemd (PID1) + sshd. Provisioning + unit start are driven later by the control script.
run_node() {
    _rn_role=$1; _rn_node=$2; shift 2
    ensure_net
    run d run -d \
        --name "$(ctr_name "$_rn_node")" --hostname "$_rn_node" \
        --network "$(net_name)" --network-alias "$_rn_node" \
        --privileged --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
        --tmpfs /run --tmpfs /run/lock --stop-signal SIGRTMIN+3 \
        --restart no \
        --label sim.instance="$INSTANCE" --label sim.role="$_rn_role" --label sim.nodeid="$_rn_node" \
        -v "$(vol_etc "$_rn_node"):/etc/tether" \
        -v "$(vol_lib "$_rn_node"):/var/lib/tether" \
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
