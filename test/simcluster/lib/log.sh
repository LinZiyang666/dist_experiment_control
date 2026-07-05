# lib/log.sh — logging + command helpers for the simcluster control script. POSIX sh.
# Sourced, never executed. No side effects at source time.

# Colorize only on a TTY.
if [ -t 2 ]; then
    _C_RED=$(printf '\033[31m'); _C_GRN=$(printf '\033[32m'); _C_YEL=$(printf '\033[33m')
    _C_BLU=$(printf '\033[34m'); _C_DIM=$(printf '\033[2m'); _C_RST=$(printf '\033[0m')
else
    _C_RED=; _C_GRN=; _C_YEL=; _C_BLU=; _C_DIM=; _C_RST=
fi

log()  { printf '%s[simcluster]%s %s\n' "$_C_BLU" "$_C_RST" "$*" >&2; }
ok()   { printf '%s[ ok ]%s %s\n'      "$_C_GRN" "$_C_RST" "$*" >&2; }
warn() { printf '%s[warn]%s %s\n'      "$_C_YEL" "$_C_RST" "$*" >&2; }
err()  { printf '%s[err ]%s %s\n'      "$_C_RED" "$_C_RST" "$*" >&2; }
die()  { err "$*"; exit 1; }

# run <cmd...>: echo the command dimly, then run it. Fails loudly.
run() {
    printf '%s+ %s%s\n' "$_C_DIM" "$*" "$_C_RST" >&2
    "$@"
}

# poll_until <timeout_s> <interval_s> <desc> -- <cmd...>: poll <cmd> until it exits 0 or timeout.
# Replaces every fixed sleep (CLAUDE.md §7 flake discipline). Returns 0 on success, 1 on timeout.
poll_until() {
    _pu_timeout=$1; _pu_interval=$2; _pu_desc=$3; shift 3
    [ "$1" = "--" ] && shift
    _pu_end=$(( $(date +%s) + _pu_timeout ))
    while :; do
        if "$@" >/dev/null 2>&1; then return 0; fi
        if [ "$(date +%s)" -ge "$_pu_end" ]; then
            warn "poll_until: timed out after ${_pu_timeout}s waiting for: ${_pu_desc}"
            return 1
        fi
        sleep "$_pu_interval"
    done
}
