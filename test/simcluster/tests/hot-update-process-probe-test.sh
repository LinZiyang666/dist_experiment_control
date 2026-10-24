#!/bin/sh
# hot-update-process-probe-test.sh — run-all entry for independent process/image controls (S1/S3).
# Real local Python children/pipes only; never starts tether, Docker or a server.
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"
# Some developer Python distributions omit os.pidfd_open even on Linux.
# Run the actual cleanup controls with a capable interpreter, never skip them.
for _hup_python in "${PYTHON3:-python3}" /usr/bin/python3; do
    if "$_hup_python" -c 'import os, signal; assert hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal")' >/dev/null 2>&1; then
        exec "$_hup_python" -I -B "$HERE/hot-update-process-probe-test.py"
    fi
done
printf '%s\n' 'hot-update probe controls require Linux Python with pidfd support' >&2
exit 2
