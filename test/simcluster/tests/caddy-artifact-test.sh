#!/bin/sh
# Hermetic Caddy supply/gate/provisioning tests; no Docker, network or real server.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
. "$HERE/../lib/supervisor.sh"
CADDY_SUPERVISOR_DIR=
trap '[ -z "$CADDY_SUPERVISOR_DIR" ] || rm -rf "$CADDY_SUPERVISOR_DIR"' EXIT
if [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ]; then
    CADDY_SUPERVISOR_DIR=$(mktemp -d)
    SIM_UPGRADE_SUPERVISOR=$(sim_supervisor_build "$(cd "$HERE/../../.." && pwd)" "$CADDY_SUPERVISOR_DIR")
    export SIM_UPGRADE_SUPERVISOR
fi
[ -x "$SIM_UPGRADE_SUPERVISOR" ] || { echo "Caddy artifact tests: shared supervisor is required" >&2; exit 2; }
# The real support-source fixture builds children, including when this entry is
# invoked directly instead of through run-all. Keep custody before Python starts.
# origin: S9 internal review FIX-CADDYART F1
"$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B "$HERE/caddy_artifact_test.py" "$@"
