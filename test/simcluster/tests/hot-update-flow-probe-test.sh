#!/bin/sh
# origin: hot-update S5 plan §8 / T47. Hermetic oracle checks only.
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"
exec "${PYTHON3:-python3}" -I -B "$HERE/hot-update-flow-probe-test.py" "$@"
