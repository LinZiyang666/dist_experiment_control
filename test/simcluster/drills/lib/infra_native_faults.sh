#!/bin/sh
# Deployment-only cable-pull adapter. Never run by hermetic tests.
# origin: hot-update S9 plan §3.2 / §9.1 / L22; S9 INSTRUMENTS-3.
set -eu
HERE=$1; SIM=$2; operation=$3; point=$4; shift 4
[ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || exit 2
case "$HERE:$SIM" in /*:/*) ;; *) exit 2 ;; esac
case "${INSTANCE:-}" in ''|*[!a-zA-Z0-9_-]*) exit 2 ;; esac
. "$HERE/lib/docker.sh"
. "$HERE/lib/log.sh"
. "$HERE/drills/lib/fault.sh"
evidence=; nodes=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --evidence) evidence=$2; shift 2 ;;
        --point) [ "$2" = "$point" ] || exit 2; shift 2 ;;
        --nodes) nodes=$2; shift 2 ;;
        --cleanup) shift ;;
        *) exit 2 ;;
    esac
done
[ "$nodes" = 'brk1 brk2 brk3' ] || exit 2
case "$evidence" in /*) ;; *) exit 2 ;; esac
# Healing uses a checked flush. fault_partition_off's best-effort return is
# appropriate for a drill EXIT trap but cannot certify a successful heal port.
case "$point:$operation" in
    nats-lost-quorum:install)
        # Partial installation failure must heal both nodes before propagating.
        trap 'dexec brk2 -- iptables -F SIMFAULT >/dev/null 2>&1 || :; dexec brk3 -- iptables -F SIMFAULT >/dev/null 2>&1 || :' EXIT HUP INT TERM
        fault_partition_on brk2 6222 >&2
        fault_partition_on brk3 6222 >&2
        trap - EXIT HUP INT TERM ;;
    nats-stream-lag:install|nats-consumer-lag:install)
        trap 'dexec brk3 -- iptables -F SIMFAULT >/dev/null 2>&1 || :' EXIT HUP INT TERM
        fault_partition_on brk3 6222 >&2
        trap - EXIT HUP INT TERM ;;
    nats-lost-quorum:heal)
        dexec brk2 -- iptables -F SIMFAULT >/dev/null
        dexec brk3 -- iptables -F SIMFAULT >/dev/null ;;
    nats-stream-lag:heal|nats-consumer-lag:heal)
        dexec brk3 -- iptables -F SIMFAULT >/dev/null ;;
    *) exit 2 ;;
esac
# A receipt records the syscall operation only. The separate status instrument
# must still prove actual group lag/quorum loss before any oracle accepts it.
exec "$SIM_UPGRADE_SUPERVISOR" -- /usr/bin/python3 -I -B -c '
import json,sys,time
point,operation=sys.argv[1:]
if operation=="install":
    row=dict(point=point,installed=True,nodes=["brk1","brk2","brk3"],at_ns=time.monotonic_ns(),mechanism="fault_partition_on:6222")
else:
    row=dict(point=point,healed=True,topology="quorum",nodes=["brk1","brk2","brk3"],at_ns=time.monotonic_ns())
print(json.dumps(row))' "$point" "$operation"
