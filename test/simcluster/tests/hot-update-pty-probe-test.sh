#!/bin/sh
# origin: hot-update S4 plan §8 T48. Hermetic, no installed tether or Docker.
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"
exec "${PYTHON3:-python3}" -I -B "$HERE/hot-update-pty-probe-test.py" "$@"
