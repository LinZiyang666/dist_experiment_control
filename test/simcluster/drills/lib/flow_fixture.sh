#!/bin/sh
# origin: hot-update S5 plan §9.1 / DRILL-SUBSHELL.
# The keeper owns remote workloads and sealed descriptors until cleanup. Its
# job belongs to the drill's main shell, never an assertion's subshell.
flow_fixture() {
    [ "$#" -ge 1 ] || return 2
    case "$1" in
        selftest)
            python3 -I -B "$HERE/drills/lib/flow_fixture.py" selftest
            ;;
        online)
            "$SIM" ctl -- node ls --json | jq -e '[.nodes[]? | select(.nid=="agt1" and .status=="ONLINE")] | length==1' >/dev/null
            ;;
        prepare)
            [ "$#" = 2 ] && [ -n "${FLOW_EV:-}" ] && [ -n "${FLOW_NONCE:-}" ] || return 2
            [ -z "${FLOW_FIXTURE_JOB:-}" ] || return 2
            python3 -I -B "$HERE/drills/lib/flow_fixture.py" serve --root "$FLOW_EV" --nonce "$FLOW_NONCE" \
                --profile "$2" --arm "${ARM:?}" --here "$HERE" --sim "$SIM" --instance "${INSTANCE:?}" \
                > "$FLOW_EV/fixture.stdout" 2> "$FLOW_EV/fixture.stderr" &
            FLOW_FIXTURE_JOB=$!
            _fff_rc=0
            python3 -I -B "$HERE/drills/lib/flow_fixture.py" call --root "$FLOW_EV" prepare || _fff_rc=$?
            [ "$_fff_rc" = 0 ] || return "$_fff_rc"
            FLOW_OPEN_IP=$(jq -er '.open.ip' "$FLOW_EV/fixture.json") || return 2
            FLOW_OPEN_PORT=$(jq -er '.open.port' "$FLOW_EV/fixture.json") || return 2
            ;;
        cleanup)
            [ -n "${FLOW_FIXTURE_JOB:-}" ] || return 0
            _fff_rc=0
            python3 -I -B "$HERE/drills/lib/flow_fixture.py" call --root "$FLOW_EV" cleanup || _fff_rc=$?
            wait "$FLOW_FIXTURE_JOB" || _fff_rc=$?
            FLOW_FIXTURE_JOB=
            return "$_fff_rc"
            ;;
        saturation|start|baseline|carrier-baseline|open|wait-event|revoke|arm-revoke|checkpoint|finish|collect|continuity|watchdog-fault)
            [ -n "${FLOW_FIXTURE_JOB:-}" ] || return 2
            python3 -I -B "$HERE/drills/lib/flow_fixture.py" call --root "$FLOW_EV" "$@"
            ;;
        *) printf '%s\n' 'flow fixture: unknown command' >&2; return 2 ;;
    esac
}
