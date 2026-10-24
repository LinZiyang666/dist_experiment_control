#!/bin/sh
# origin: hot-update S5 plan §9.1 / T47.
# Source after setting HERE to the simcluster root. The independent byte
# oracle and the live fixture below have separate judging/lifecycle owners.
# Secret master/cipher captures use inherited sealed memfds, never argv/env.

flow_probe() {
    "${PYTHON3:-python3}" -I -B "$HERE/drills/lib/hot_update_flow_probe.py" "$@"
}

flow_continuity_judge() {
    [ "$#" = 2 ] || return 2
    # Keep the native 0/1/2 status for normal runs AND negative controls.
    # L's native safe refusal must retain the carrier. The workload never
    # selects this exception; only the orchestrator's fixed arm does.
    if [ "${ARM:-}" = L ]; then
        flow_probe judge --evidence "$1" --nonce "$2" --carrier-transition unchanged
    else
        flow_probe judge --evidence "$1" --nonce "$2"
    fi
}

flow_describe_capture() {
    [ "$#" = 4 ] || return 2
    flow_probe describe --raw "$1" --arrivals "$2" --output "$3" --nonce "$4"
}

flow_process_identity() {
    [ "$#" = 1 ] || return 2
    flow_probe identity --pid "$1"
}

flow_socket_identity() {
    [ "$#" = 3 ] || return 2
    flow_probe identity --pid "$1" --fd "$2" --app-nonce "$3"
}

# The byte oracle and the deploy collector have separate responsibilities.
# Sourcing this file supplies both interfaces consumed by 75 and 39.F.
. "$HERE/drills/lib/flow_fixture.sh"
