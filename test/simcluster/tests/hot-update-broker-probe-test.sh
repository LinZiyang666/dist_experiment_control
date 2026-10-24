#!/bin/sh
# origin: hot-update S6 plan §10.1 / §12.1 P29.
# Hermetic only: fake CLI process boundary, real pipes/sockets/wait, no deploy.
set -eu
_broker_test_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec "${PYTHON3:-python3}" -I -B "$_broker_test_dir/hot-update-broker-probe-test.py" "$@"
