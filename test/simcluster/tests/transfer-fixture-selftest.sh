#!/bin/sh
# origin: hot-update S7 plan §10.4 / P29; S6 DRILL-ASSETS real startup seam.
set -eu
TRANSFER_SELFTEST_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TRANSFER_SELFTEST_SUPERVISOR_DIR=
trap '[ -z "$TRANSFER_SELFTEST_SUPERVISOR_DIR" ] || rm -rf "$TRANSFER_SELFTEST_SUPERVISOR_DIR"' EXIT
if [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ]; then
    . "$TRANSFER_SELFTEST_DIR/../lib/supervisor.sh"
    TRANSFER_SELFTEST_SUPERVISOR_DIR=$(mktemp -d)
    TRANSFER_SELFTEST_REPO=$(CDPATH= cd -- "$TRANSFER_SELFTEST_DIR/../../.." && pwd)
    SIM_UPGRADE_SUPERVISOR=$(sim_supervisor_build "$TRANSFER_SELFTEST_REPO" "$TRANSFER_SELFTEST_SUPERVISOR_DIR")
    export SIM_UPGRADE_SUPERVISOR
fi
python3 -I -B "$TRANSFER_SELFTEST_DIR/transfer-fixture-selftest.py" "$@"
