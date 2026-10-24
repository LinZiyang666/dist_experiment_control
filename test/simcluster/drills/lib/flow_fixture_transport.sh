#!/bin/sh
# origin: hot-update S5 plan §9.1. Keep container and log mapping in their
# existing owners. Tests replace this executable at the process boundary.
set -eu
# The shared local owner also contains short copy/log tools. Long remote work
# uses fullstack-exec below: a local docker client is not a remote deathwatch.
# origin: hot-update S10 plan §6 / §10.2 P11.
if [ "${FLOW_TRANSPORT_SUPERVISED:-0}" != 1 ]; then
    [ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || exit 2
    export FLOW_TRANSPORT_SUPERVISED=1
    exec "$SIM_UPGRADE_SUPERVISOR" -- /bin/sh "$0" "$@"
fi
HERE=$1; INSTANCE=$2; SIM=$3; shift 3
. "$HERE/lib/docker.sh"
. "$HERE/drills/lib/logs.sh"
# S10 uses this adapter only to supply/start original actors before READY.
# Ongoing product commands belong to P18, and P16 closes existing collectors.
# Generic legacy exec/input must not become a post-READY repair escape hatch.
if [ "${FULLSTACK_PROFILE:-0}" = 1 ]; then
    case "$1" in
        fullstack-exec|fullstack-exec-user|copy-to)
            [ -n "${FULLSTACK_READY_FILE:-}" ] || exit 2
            [ ! -e "$FULLSTACK_READY_FILE" ] && [ ! -L "$FULLSTACK_READY_FILE" ] || exit 2 ;;
        copy-from|ip|logs|fullstack-peer-call|fullstack-peer-read) ;;
        *) exit 2 ;;
    esac
fi
flow_fixture_transport_online() {
    "$SIM" ctl -- node ls --json | jq -e '[.nodes[]? | select(.nid=="agt1" and .status=="ONLINE")] | length==1' >/dev/null
}
flow_fixture_transport_ready() {
    # Observer installation restarts the real services. Readiness is an
    # observed precondition for FIRST business, never an expose retry.
    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.C
    if [ "$_fft_profile" = capacity-32 ]; then
        "$SIM" exec brk1 -- /usr/local/bin/tether cluster status --json | jq -e '
            .health=="HEALTHY_HA" and .partial==false and (.errors|length)==0 and
            (.leader_id|type=="string" and length>0) and
            ([.nodes[]? | select(.phase=="VOTER" and .reachable==true and
                .applied_lag==0 and .inconsistent==false)] | length)==3' >/dev/null || return 1
    else
        "$SIM" exec brk1 -- /usr/local/bin/tether cluster status --json | jq -e '.leader_id=="brk1"' >/dev/null || return 1
    fi
    flow_fixture_transport_online
}
case "$1" in
    fullstack-peer-call|fullstack-peer-read)
        # These closed verbs remain available after READY for the original
        # keepers only; no arbitrary exec/install escape is opened for P12.
        # origin: hot-update S10 plan §3.2 / §6 / P11-P12 seam.
        _fft_op=$1; _fft_node=$2; _fft_root=$3
        case "$_fft_node" in agt1|agt2|ctl1) ;; *) exit 2 ;; esac
        case "$_fft_root" in /*) ;; *) exit 2 ;; esac
        case "$_fft_root" in *[!a-zA-Z0-9_./-]*|*/../*|*/..|/) exit 2 ;; esac
        if [ "$_fft_op" = fullstack-peer-call ]; then
            d exec -i "$(ctr_name "$_fft_node")" "$_fft_root/supervise" -- \
                /usr/bin/env "SIM_UPGRADE_SUPERVISOR=$_fft_root/supervise" \
                /usr/bin/python3 -I -B "$_fft_root/flow_fixture_peer.py" call --root "$_fft_root"
        else
            case "$4" in ready.json|*.sent|*.received) ;; *) exit 2 ;; esac
            case "$4" in *[!a-z0-9.-]*|*..*|.*) exit 2 ;; esac
            d exec "$(ctr_name "$_fft_node")" "$_fft_root/supervise" -- /bin/cat "$_fft_root/$4"
        fi
        ;;
    fullstack-exec|fullstack-exec-user)
        # P19/P12 supply the pre-registered remote cgroup and heartbeat stdin.
        # No shell interpolation of command payloads; the remote launcher owns
        # descendants when the local transport or its owner disappears.
        _fft_op=$1; shift; _fft_node=$1; _fft_cgroup=$2; shift 2
        case "$_fft_node" in agt1|agt2|brk1|brk2|brk3|ctl1) ;; *) exit 2 ;; esac
        [ -n "$_fft_cgroup" ] || exit 2
        if [ "$_fft_op" = fullstack-exec-user ]; then
            _fft_user=$1; shift
            case "$_fft_user" in ''|-*|*[!a-z0-9_-]*) exit 2 ;; esac
            set -- -u "$_fft_user" "$(ctr_name "$_fft_node")" /usr/local/bin/tether-test-supervise \
                --lease-fd 0 --lease-timeout 5s --cgroup "$_fft_cgroup" -- "$@"
            d exec -i "$@"
        else
        d exec -i "$(ctr_name "$_fft_node")" /usr/local/bin/tether-test-supervise \
            --lease-fd 0 --lease-timeout 5s --cgroup "$_fft_cgroup" -- "$@"
        fi
        ;;
    policy)
        [ -z "${FULLSTACK_READY_FILE:-}" ] || [ ! -e "$FULLSTACK_READY_FILE" ] || exit 2
        [ "${FULLSTACK_PROFILE:-0}" != 1 ] || exit 2
        . "$HERE/lib/log.sh"
        . "$HERE/lib/tether.sh"
        . "$HERE/drills/lib/agentyaml.sh"
        . "$HERE/drills/lib/upgradecfg.sh"
        # The probe's RFC1918 sink needs the operator's explicit policy before
        # FIRST business. Reuse the deploy provisioning and allowlist owners.
        # Fixed phase markers locate a failed provisioning boundary without
        # reflecting config, private argv, or changing its native status.
        # origin: hot-update S5 plan §9.4 / DRILL-RUN 75.B
        # A failed start must retain its native status and expose only the
        # fixed systemd result. This diagnostic never resets or restarts a unit.
        # origin: hot-update S5 plan §9.4 / DRILL-RUN 75.R
        flow_fixture_policy_exit() {
            _fft_policy_rc=$?
            trap - EXIT
            if [ "$_fft_policy_rc" -ne 0 ]; then
                _fft_policy_result=$(dexec agt1 -- systemctl show tether-agent.service -p Result --value 2>/dev/null) || _fft_policy_result=unavailable
                case "$_fft_policy_result" in
                    success|start-limit-hit|exit-code|signal|timeout|resources|dependency|unavailable)
                        printf 'FLOW_POLICY_RESULT=%s\n' "$_fft_policy_result" >&2 ;;
                esac
            fi
            exit "$_fft_policy_rc"
        }
        trap flow_fixture_policy_exit EXIT
        printf '%s\n' 'FLOW_POLICY_PHASE=provision' >&2
        agent_provision_yaml agt1 lab nats://brk1:4222 proxyprivate
        printf '%s\n' 'FLOW_POLICY_PHASE=allowlist' >&2
        upgradecfg_allow_agent agt1 flow_fixture_transport_online lab
        ;;
    observe-start)
        [ -z "${FULLSTACK_READY_FILE:-}" ] || [ ! -e "$FULLSTACK_READY_FILE" ] || exit 2
        [ "${FULLSTACK_PROFILE:-0}" != 1 ] || exit 2
        _fft_profile=$2; _fft_nonce=$3
        case "$_fft_profile" in legacy-exporter) exit 0 ;; esac
        # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN.
        # Force real short cipher writes on the local SS/flow TCP link as
        # well as the external fixture sockets. Loopback's 64 KiB segments
        # otherwise absorb the bounded pressure without a positive short n.
        # This is a network fault in this disposable namespace; no product
        # socket, credit, timeout, or observation result is patched.
        dexec agt1 -- ip link set dev lo mtu 1500
        dexec agt1 -- python3 -I -B -c 'import json,pathlib; mtu=int(pathlib.Path("/sys/class/net/lo/mtu").read_text()); assert mtu==1500; print(json.dumps({"cipher_pressure_loopback_mtu":mtu}))'
        _fft_nodes='agt1 brk1'
        [ "$2" != capacity-32 ] || _fft_nodes='agt1 brk1 brk2 brk3'
        for _fft_node in $_fft_nodes; do
            [ "$_fft_profile" != legacy-broker ] || [ "$_fft_node" != brk1 ] || continue
            for _fft_script in flow_observation.py flow_observer_service.py flow_fixture_peer.py hot_update_flow_probe.py; do
                d cp "$HERE/drills/lib/$_fft_script" "$(ctr_name "$_fft_node"):/tmp/$_fft_script"
            done
            d cp "$SIM_UPGRADE_SUPERVISOR" "$(ctr_name "$_fft_node"):/usr/local/bin/tether-test-supervise"
            dexec "$_fft_node" -- python3 -I -B /tmp/flow_observer_service.py --node "$_fft_node" --nonce "$_fft_nonce" --script /tmp/flow_observation.py
        done
        . "$HERE/lib/log.sh"
        poll_until 30 1 'observed cluster and agent readiness' -- flow_fixture_transport_ready
        ;;
    observe-stop)
        # S10 closes observation through P16/P05 only. The legacy stop verb
        # restarts products and therefore cannot be used inside its READY span.
        [ -z "${FULLSTACK_READY_FILE:-}" ] || [ ! -e "$FULLSTACK_READY_FILE" ] || exit 2
        [ "${FULLSTACK_PROFILE:-0}" != 1 ] || exit 2
        [ "$2" != legacy-exporter ] || exit 0
        _fft_nodes='agt1 brk1'
        [ "$2" != capacity-32 ] || _fft_nodes='agt1 brk1 brk2 brk3'
        for _fft_node in $_fft_nodes; do
            [ "$2" != legacy-broker ] || [ "$_fft_node" != brk1 ] || continue
            _fft_unit=tether-agent.service
            [ "$_fft_node" = agt1 ] || _fft_unit=tether-broker.service
            dexec "$_fft_node" -- python3 -I -B -c 'import pathlib,subprocess,sys; unit=sys.argv[1]; subprocess.run(["systemctl","stop",unit],check=True); (pathlib.Path("/etc/systemd/system")/(unit+".d")/"zz-flow-observer.conf").unlink(missing_ok=True); subprocess.run(["systemctl","daemon-reload"],check=True); subprocess.run(["systemctl","start",unit],check=True)' "$_fft_unit"
        done
        ;;
    exec) shift; _fft_node=$1; shift; dexec "$_fft_node" -- "$@" ;;
    exec-user) shift; _fft_node=$1; shift; dexec -u sim "$_fft_node" -- "$@" ;;
    input) shift; _fft_node=$1; shift; d exec -i "$(ctr_name "$_fft_node")" "$@" ;;
    copy-to) d cp "$2" "$(ctr_name "$3"):$4" ;;
    copy-from) d cp "$(ctr_name "$2"):$3" "$4" ;;
    ctl) shift; "$SIM" ctl -- "$@" ;;
    ip) d inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$(ctr_name "$2")" ;;
    logs) sim_log_snapshot "$2" "$3" "$4" "$5" ;;
    *) exit 2 ;;
esac
