#!/bin/sh
# assert-shell-state-test.sh — hermetic capture ownership and source-guard controls.
# origin: hot-update S4 plan §9.1–§9.3 / DRILL-SUBSHELL.
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"
exec python3 -I -B "$HERE/assert-shell-state-test.py"
