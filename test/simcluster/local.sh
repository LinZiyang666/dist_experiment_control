#!/usr/bin/env bash
# local.sh — on-host driver. Use this when you ARE on the sim host (weilandserver), which since the
# 2026-07-25 move is also the main development box. It does what remote.sh does minus the two things
# that only make sense across machines: no rsync (the tree is already here) and no ssh (just exec).
#
# Relationship to remote.sh:
#   local.sh   on weilandserver          build in place  → run ./simcluster directly
#   remote.sh  on any other box          build + rsync   → ssh and run ./simcluster there
# Both stage vendor/ through lib/stage.sh, so the nats-server pin cannot drift between them.
#
# Usage: ./local.sh [--build] <simcluster subcommand + args...>
#        ./local.sh --build build
#        ./local.sh up --brokers 3 --agents 1 --ctl 1
#        ./local.sh drill 10-grow-to-3
#        ./local.sh drill-all -j6           # dispatches to run-drills.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
export SIM_REPO="$(cd "$HERE/../.." && pwd)"
export SIM_VENDOR="$HERE/vendor"
export SIM_TAG="local"
# shellcheck source=lib/stage.sh
. "$HERE/lib/stage.sh"

# --- refuse to run on the wrong machine -------------------------------------------------------
# This spins real privileged containers and writes to real persistent volumes. Running it on a
# laptop/WSL box by mistake would either fail confusingly or start a second cluster somewhere it
# does not belong, so make the wrong-host case loud instead of surprising. SIM_ALLOW_ANY_HOST=1 is
# the deliberate escape hatch (a new sim host, or a rename before the docs catch up).
if ! sim_is_sim_host && [ "${SIM_ALLOW_ANY_HOST:-0}" != "1" ]; then
    cat >&2 <<EOF
[local] this machine is not the sim host.
        hostname : $(hostname 2>/dev/null || echo '?')
        expected : weilandserver / weiland_server, or IP ${SIM_HOST_IP:-192.168.1.150}

        From another box use the external driver instead:
            ./remote.sh ${*:-status}

        If this really is a (new) sim host, re-run with SIM_ALLOW_ANY_HOST=1.
EOF
    exit 2
fi

command -v docker >/dev/null 2>&1 || {
    echo "[local] docker not on PATH — the sim host needs Docker (systemd-in-docker: --privileged --cgroupns=host)" >&2
    exit 1
}

do_build=0
if [ "${1:-}" = "--build" ]; then do_build=1; shift; fi
if [ "$do_build" = "1" ]; then sim_stage_binaries; fi

[ -x "$SIM_VENDOR/tether" ] || {
    echo "[local] no vendor/tether — run './local.sh --build build' first" >&2
    exit 1
}

[ "$#" -gt 0 ] || exit 0

# `drill-all` dispatches to run-drills.sh (parallel whole-suite runner); everything else to simcluster.
entry="$HERE/simcluster"
if [ "$1" = "drill-all" ]; then entry="$HERE/run-drills.sh"; shift; fi

# No printf %q dance here: remote.sh needs it because argv has to survive a remote shell. Locally we
# exec directly, so argv boundaries are preserved by the kernel and nothing is re-parsed.
echo "[local] ${entry##*/} $*" >&2
cd "$HERE"
exec "$entry" "$@"
