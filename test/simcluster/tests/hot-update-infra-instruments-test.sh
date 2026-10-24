#!/bin/sh
# Hermetic admission only; the real candidate executable is pinned, never run.
# origin: hot-update S9 plan §9.1 / §9.4; S9 INSTRUMENTS.
set -eu
_instruments_here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
_instruments_repo=$(CDPATH= cd -- "$_instruments_here/../../.." && pwd)
_instruments_tmp=$(mktemp -d /tmp/tether-instrument-tests.XXXXXXXX)
trap 'rm -rf "$_instruments_tmp"' EXIT HUP INT TERM
. "$_instruments_here/../lib/supervisor.sh"
if [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ]; then
    SIM_UPGRADE_SUPERVISOR=$(sim_supervisor_build "$_instruments_repo" "$_instruments_tmp")
    export SIM_UPGRADE_SUPERVISOR
fi
if [ -z "${SIM_INFRA_TEST_BINARY:-}" ]; then
    SIM_INFRA_TEST_BINARY="$_instruments_tmp/tether-candidate"
    ( cd "$_instruments_repo" && "$SIM_UPGRADE_SUPERVISOR" -- \
        go build -o "$SIM_INFRA_TEST_BINARY" ./cmd/tether )
    export SIM_INFRA_TEST_BINARY
fi
"$SIM_UPGRADE_SUPERVISOR" -- /usr/bin/python3 -I -B \
    "$_instruments_here/hot-update-infra-instruments-test.py" "$@"
