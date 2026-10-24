#!/bin/sh
# Hermetic P32 entry: isolated Python, no root/network/deployment/downloads.
# origin: hot-update S9 plan §9.1 / §12 P32 / T76 / T77
set -eu
_infra_test_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
_infra_test_repo=$(CDPATH= cd -- "$_infra_test_dir/../../.." && pwd)
_infra_test_tmp=$(mktemp -d /tmp/tether-infra-selftest.XXXXXXXX)
trap 'rm -rf "$_infra_test_tmp"' EXIT HUP INT TERM
. "$_infra_test_dir/../lib/supervisor.sh"
if [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ]; then
    SIM_UPGRADE_SUPERVISOR=$(sim_supervisor_build "$_infra_test_repo" "$_infra_test_tmp")
    export SIM_UPGRADE_SUPERVISOR
fi
[ -x "$SIM_UPGRADE_SUPERVISOR" ] || exit 2
"$SIM_UPGRADE_SUPERVISOR" -- "${PYTHON3:-python3}" -I -B \
    "$_infra_test_dir/hot-update-infra-probe-test.py" "$@"
# origin: S9 FIX-INFRAVERBS. Verb grammar, custody, order and log-scope controls.
"$SIM_UPGRADE_SUPERVISOR" -- "${PYTHON3:-python3}" -I -B \
    "$_infra_test_dir/hot-update-infra-verbs-test.py"
