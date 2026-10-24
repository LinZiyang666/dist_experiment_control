#!/bin/sh
# origin: hot-update S10 plan §5.5 / §10.2 P09. Hermetic files/journal protocol;
# no Docker, systemd, installed binary, production HOME or unsupervised child.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
. "$HERE/../drills/lib/logs.sh"
FULLSTACK_LOGS_PY=$HERE/../drills/lib/fullstack_logs.py
_flst_python=${PYTHON3:-python3}

# The shell seam is tested without spawning an extra command. The reader's
# native rc must survive, and paths with whitespace remain individual argv.
_fullstack_logs_test_reader() {
    [ "$1" = -I ] && [ "$2" = -B ] && [ "$3" = "$FULLSTACK_LOGS_PY" ] || exit 1
    shift 3
    [ "$1" = "$_flst_action" ] && [ "$2" = 'roster cursor.json' ] || exit 1
    case "$1" in
        start|verify) [ "$#" = 3 ] && [ "$3" = 'end cursor.json' ] || exit 1 ;;
        drain) [ "$#" = 4 ] && [ "$3" = 'raw directory' ] && [ "$4" = 'end cursor.json' ] || exit 1 ;;
    esac
    return 75
}
PYTHON3=_fullstack_logs_test_reader
for _flst_action in start drain verify; do
    _flst_rc=0
    case "$_flst_action" in
        start) fullstack_logs_start 'roster cursor.json' 'end cursor.json' || _flst_rc=$? ;;
        drain) fullstack_logs_drain 'roster cursor.json' 'raw directory' 'end cursor.json' || _flst_rc=$? ;;
        verify) fullstack_logs_verify 'roster cursor.json' 'end cursor.json' || _flst_rc=$? ;;
    esac
    [ "$_flst_rc" = 75 ] || {
        printf '%s\n' 'strict log shell adapter lost native reader rc' >&2
        exit 1
    }
done
unset PYTHON3
for _flst_action in fullstack_logs_start fullstack_logs_drain fullstack_logs_verify; do
    _flst_rc=0
    "$_flst_action" || _flst_rc=$?
    [ "$_flst_rc" = 2 ] || exit 1
done
# origin: S10 internal review rv-honesty F6
# P05 is supplied by the caller; no unsupervised build or PATH fallback.
case "${FULLSTACK_SUPERVISE:-}" in
    /*) [ -x "$FULLSTACK_SUPERVISE" ] || exit 2 ;;
    *) printf '%s\n' 'FULLSTACK_SUPERVISE must name the built P05 launcher' >&2; exit 2 ;;
esac

exec "$FULLSTACK_SUPERVISE" -- "$_flst_python" -I -B "$HERE/../drills/lib/fullstack_logs_test.py" "$@"
