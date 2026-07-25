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

# _pu_push <end> <timeout> <interval> <mode> <desc> : append one frame (newline separated, top = last).
# External review Major 1: MODE (fast|fixed) MUST live in the frame, not a process-global. A nested poll
# of a DIFFERENT mode inside a predicate would otherwise overwrite the global and the OUTER poll would
# resume on the inner's grid — silently flipping poll_until_fixed to fast (extra product mutations / a
# banked transient) or vice-versa. mode sits BEFORE desc, which stays last and may contain '|'.
_pu_push() { _PU_STACK="$_PU_STACK$1|$2|$3|$4|$5
"; }
# _pu_peek : restore _pu_end/_pu_timeout/_pu_interval/_pu_mode/_pu_desc from the TOP frame.
_pu_peek() {
    _pu_top=$(printf '%s' "$_PU_STACK" | tail -n 1)
    _pu_end=${_pu_top%%|*};      _pu_top=${_pu_top#*|}
    _pu_timeout=${_pu_top%%|*};  _pu_top=${_pu_top#*|}
    _pu_interval=${_pu_top%%|*}; _pu_top=${_pu_top#*|}
    _pu_mode=${_pu_top%%|*};     _pu_desc=${_pu_top#*|}
}
# _pu_pop : drop the TOP frame.
_pu_pop() { _PU_STACK=$(printf '%s' "$_PU_STACK" | sed '$d'); [ -z "$_PU_STACK" ] || _PU_STACK="$_PU_STACK
"; }

# poll_until <timeout> <interval> <desc> -- <cmd> : poll with a FAST-START sampling grid (V2). The
# <interval> is a CAP: for the first <interval> seconds the predicate is sampled every min(1s, interval),
# then at the declared interval. A condition that resolves in ~1s is caught at ~1s instead of waiting a
# full 2-3s grid step — the measured overshoot bucket the M3 POLL-WAIT trailers quantify. The DEADLINE is
# unchanged (the budget is the contract; the interval is only the sampling cadence).
#
# WHY 1s and not 200ms geometric: poll_until returns at FIRST success, so denser early sampling raises the
# chance of banking a TRANSIENTLY-true state (the #46 leader-flap class). A 1s floor stays at or coarser
# than the current 2-3s grid's own resolution, so it cannot catch a sub-second transient the old grid
# would have missed — the speedup is real but the transient-catch risk is not increased. Predicates where
# even 1s matters (a mutation per tick, or a "stays true for a window" oracle) use poll_until_fixed.
poll_until()       { _poll_impl fast "$@"; }
# poll_until_fixed <timeout> <interval> <desc> -- <cmd> : the exact fixed grid, for the two audited
# exemption classes — (a) EFFECTFUL predicates that issue a product mutation per tick (faster sampling =
# more mutations = a different load profile), and (b) STABILITY-WINDOW / settle waits where the interval
# IS the oracle (a `-- false` timer, or "the distribution has stopped moving").
poll_until_fixed() { _poll_impl fixed "$@"; }

_poll_impl() {
    _pi_mode=$1; shift
    # N-9 (external review): flatten newlines in the description. The frame stack (_pu_push/_pu_peek) is
    # line-encoded, so a desc carrying a literal newline would corrupt the stack and the deadline re-read,
    # turning the poll into an UNBOUNDED busy-wait. A single-line desc can never do that.
    _pu_timeout=$1; _pu_interval=$2; _pu_desc=$(printf '%s' "$3" | tr '\n' ' '); shift 3
    [ "$1" = "--" ] && shift
    # Major 1: push the mode into THIS frame; the sleep step below reads _pu_mode from _pu_peek, never a
    # global, so a nested poll cannot change our grid. mode must carry no '|' (it is fast|fixed).
    _pu_push "$(( $(date +%s) + _pu_timeout ))" "$_pu_timeout" "$_pu_interval" "$_pi_mode" "$_pu_desc"
    while :; do
        if "$@" >/dev/null 2>&1; then
            # M3 INSTRUMENTATION. A full sweep took 47.4 minutes on 2026-07-23 and NOTHING could say
            # where those minutes went: drill logs have no per-line timestamps and this function, used
            # at 315 sites, never recorded how long it actually blocked. (The first attempt to answer
            # the question by counting `sleep` in the drills produced "51 minutes", which was wrong by
            # 17x — it had counted `tether exec -- sleep 9663` FIXTURE PROCESS NAMES as waits. Real
            # blocking sleep across the whole suite is 175 s.) One edit here instruments every site.
            #
            # The start time is DERIVED (end - timeout) rather than stored: the frame stack is
            # line-encoded and a fifth field would change its format, which is the machinery an earlier
            # re-entrancy bug (R1/H5) already broke once. Peek BEFORE popping — a nested poll inside the
            # predicate leaves our frame on top, but the shell variables hold whatever it last read.
            _pu_peek
            _pu_el=$(( $(date +%s) - (_pu_end - _pu_timeout) ))
            [ "$_pu_el" -lt 0 ] && _pu_el=0
            _pu_accum "$_pu_el"
            # Only report waits worth reading. A poll that returns on its first sample is the common
            # case and logging it would bury the ones that cost real time.
            [ "$_pu_el" -ge 5 ] && log "poll_until: condition met after ${_pu_el}s (budget ${_pu_timeout}s): ${_pu_desc}"
            _pu_pop; return 0
        fi
        _pu_peek                      # <cmd> may have nested; re-read OUR frame before judging the deadline
        if [ "$(date +%s)" -ge "$_pu_end" ]; then
            warn "poll_until: timed out after ${_pu_timeout}s waiting for: ${_pu_desc}"
            # MA5: add the REAL elapsed, not the nominal timeout — a poll that times out early (its
            # predicate errored) did not actually block for the full budget.
            _pu_tel=$(( $(date +%s) - (_pu_end - _pu_timeout) ))
            [ "$_pu_tel" -lt 0 ] && _pu_tel=0
            _pu_accum "$_pu_tel"
            _pu_pop; return 1
        fi
        # V2 fast-start: for the first <interval> seconds sample every min(1s, interval); after that use
        # the declared interval. `fixed` mode always sleeps the declared interval. All integer-second
        # (dash-safe); the elapsed is derived statelessly from the frame (end - timeout), so nesting is
        # unaffected. _pu_end/_pu_timeout/_pu_interval were just refreshed by _pu_peek above.
        if [ "$_pu_mode" = fast ] && [ "$_pu_interval" -gt 1 ]; then
            _pu_el2=$(( $(date +%s) - (_pu_end - _pu_timeout) ))
            if [ "$_pu_el2" -lt "$_pu_interval" ]; then sleep 1; else sleep "$_pu_interval"; fi
        else
            sleep "$_pu_interval"
        fi
    done
}

# _pu_accum <seconds> : add to the wait total, but ONLY at stack depth 0 (MA5). A nested poll's time is
# ALREADY inside its parent's elapsed, so counting both double-counts (a 6 s wait at depth 2 was being
# reported as 12 s, and the trailer could exceed the drill's own wall time). At depth 0 the outermost
# poll's elapsed already subsumes every nested wait, which is the number we want.
_pu_accum() {
    # After the caller's _pu_pop has NOT yet run, this frame is still on the stack; depth 1 means we are
    # the outermost poll. Count only then.
    _pu_depth=$(printf '%s' "$_PU_STACK" | grep -c '|' 2>/dev/null || echo 0)
    [ "${_pu_depth:-0}" -le 1 ] && _PU_TOTAL=$(( ${_PU_TOTAL:-0} + $1 ))
}

# poll_wait_total : seconds this drill spent inside TOP-LEVEL poll_until calls. Reported by drill_end.
# LIMIT (MA5): a poll_until that runs INSIDE an assert predicate — e.g. `assert_ok "…" poll_until …`, or a
# helper like wait_phase used as a predicate — executes in _as_capture's command substitution (a
# subshell), so its increment never reaches this accumulator. This number therefore covers DIRECT poll
# calls only, and undercounts. It is diagnostic and has no consumer that gates a verdict; when V2 tunes
# poll intervals it must not treat this as the complete wait budget until the subshell path is captured.
poll_wait_total() { printf '%s' "${_PU_TOTAL:-0}"; }
