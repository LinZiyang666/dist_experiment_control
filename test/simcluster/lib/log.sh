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
#
# REENTRANCY (R1/H5 — this used to be broken and it mattered). POSIX sh has no `local`, so the loop state
# lived in plain globals. <cmd> is frequently a helper that ITSELF calls poll_until (drills/lib/proxy.sh:51
# ss_up, drills/lib/cluster.sh:31/71/75, drills/lib/agentyaml.sh:117/121, drills/lib/ingress.sh:43/85,
# drills/lib/ident.sh:96, and lib/tether.sh wait_phase — which is used all over the grow/retire family).
# The nested call clobbered the outer frame, producing TWO distinct failures:
#   MODE A (inner FAILS): the outer deadline was left at the inner's (shorter) end, so the outer aborted on
#     its very first iteration and reported the INNER's desc/timeout. Observed live as
#     `timed out after 10s waiting for: ss-local ... SOCKS listener ready` under an assertion that had
#     actually asked for 240s (74-rebalance-on-return).
#   MODE B (inner SUCCEEDS): every successful inner call re-armed the outer's `_pu_end` to now+inner_timeout,
#     so the outer deadline moved forward forever — an UNBOUNDED HANG, not a timeout. This is why
#     run-drills.sh also carries a per-drill `timeout` backstop now.
# Fix: keep each invocation's state in an explicit FRAME STACK. A nested call pushes and pops symmetrically,
# so after <cmd> returns, the top of the stack is still OUR frame. The predicate deliberately still runs in
# THIS shell (not a subshell): drills rely on helper functions and on predicates that set globals.
_PU_STACK=''

# _pu_push <end> <timeout> <interval> <desc> : append one frame (records are newline separated, top = last).
_pu_push() { _PU_STACK="$_PU_STACK$1|$2|$3|$4
"; }
# _pu_peek : restore _pu_end/_pu_timeout/_pu_interval/_pu_desc from the TOP frame.
_pu_peek() {
    _pu_top=$(printf '%s' "$_PU_STACK" | tail -n 1)
    _pu_end=${_pu_top%%|*};      _pu_top=${_pu_top#*|}
    _pu_timeout=${_pu_top%%|*};  _pu_top=${_pu_top#*|}
    _pu_interval=${_pu_top%%|*}; _pu_desc=${_pu_top#*|}
}
# _pu_pop : drop the TOP frame.
_pu_pop() { _PU_STACK=$(printf '%s' "$_PU_STACK" | sed '$d'); [ -z "$_PU_STACK" ] || _PU_STACK="$_PU_STACK
"; }

poll_until() {
    # N-9 (external review): flatten newlines in the description. The frame stack (_pu_push/_pu_peek) is
    # line-encoded, so a desc carrying a literal newline would corrupt the stack and the deadline re-read,
    # turning the poll into an UNBOUNDED busy-wait. A single-line desc can never do that.
    _pu_timeout=$1; _pu_interval=$2; _pu_desc=$(printf '%s' "$3" | tr '\n' ' '); shift 3
    [ "$1" = "--" ] && shift
    _pu_push "$(( $(date +%s) + _pu_timeout ))" "$_pu_timeout" "$_pu_interval" "$_pu_desc"
    while :; do
        if "$@" >/dev/null 2>&1; then _pu_pop; return 0; fi
        _pu_peek                      # <cmd> may have nested; re-read OUR frame before judging the deadline
        if [ "$(date +%s)" -ge "$_pu_end" ]; then
            warn "poll_until: timed out after ${_pu_timeout}s waiting for: ${_pu_desc}"
            _pu_pop; return 1
        fi
        sleep "$_pu_interval"
    done
}
