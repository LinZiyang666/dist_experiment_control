#!/bin/sh
# Hermetic composer contracts; never invokes a deployment tool or product.
# origin: hot-update S9 plan §9.1 / §9.4 / S9 COMPOSER.
set -eu
_composer_tests=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
_composer_repo=$(CDPATH= cd -- "$_composer_tests/../../.." && pwd)
_composer_tmp=$(mktemp -d /tmp/tether-composer-tests.XXXXXXXX)
trap 'rm -rf "$_composer_tmp"' EXIT HUP INT TERM
. "$_composer_tests/../lib/supervisor.sh"
if [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ]; then
    SIM_UPGRADE_SUPERVISOR=$(sim_supervisor_build "$_composer_repo" "$_composer_tmp")
    export SIM_UPGRADE_SUPERVISOR
fi
[ -x "$SIM_UPGRADE_SUPERVISOR" ] || exit 2
"$SIM_UPGRADE_SUPERVISOR" -- /usr/bin/python3 -I -B \
    "$_composer_tests/hot-update-infra-composer-test.py" "$@"
