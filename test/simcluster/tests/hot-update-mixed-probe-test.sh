#!/bin/sh
# hot-update-mixed-probe-test.sh — real scheduler, temporary fake ctl only.
# origin: hot-update S3 plan §8.2. No broker, Docker or installed tether.
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"
exec "${PYTHON3:-python3}" -I -B "$HERE/hot-update-mixed-probe-test.py" "$@"
