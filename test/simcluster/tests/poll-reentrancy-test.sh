#!/bin/sh
# poll-reentrancy-test.sh — R1/H5 regression gate for poll_until's frame stack.
#
# WHY THIS FILE EXISTS. `poll_until` used to keep its loop state in plain globals, and drills nest it
# constantly (drills/lib/proxy.sh:51, cluster.sh:31/71/75, agentyaml.sh:117/121, ingress.sh:43/85,
# ident.sh:96, lib/tether.sh wait_phase). The nested call clobbered the outer frame. Two distinct bugs:
#   MODE A  inner FAILS    -> outer inherits the inner's shorter deadline, aborts on iteration 1, and
#                             misreports the INNER's desc ("timed out after 10s waiting for: ss-local ...")
#                             under an assertion that had asked for 240s.
#   MODE B  inner SUCCEEDS -> every inner success re-arms the outer deadline => UNBOUNDED HANG.
#
# NON-VACUITY (roadmap §3 gate C): each case is proven to actually catch its bug by running the SAME case
# against a deliberately re-broken (pre-R1) implementation, which MUST fail. A test that cannot go red is
# not evidence. Run: sh tests/poll-reentrancy-test.sh   (also exercised under dash by the caller)
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=../lib/log.sh
. "$HERE/../lib/log.sh"
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  FAIL %s\n' "$1" >&2; }

# The pre-R1 implementation, verbatim, used only to prove the cases are non-vacuous.
broken_poll_until() {
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

# ── MODE A ───────────────────────────────────────────────────────────────────────────────────────────
# Outer asks for 6s. Its predicate nests a poll_until that always FAILS with a 1s budget. The outer must
# still poll for ~6s and, on timeout, report ITS OWN desc — not the inner's.
inner_always_fails() { poll_until 1 1 "INNER-DESC" -- false; return 1; }
inner_always_fails_broken() { broken_poll_until 1 1 "INNER-DESC" -- false; return 1; }

mode_a() {
    _impl=$1
    _t0=$(date +%s)
    _err=$("$_impl" 6 1 "OUTER-DESC" -- "$2" 2>&1)
    _t1=$(date +%s)
    printf '%s|%s' "$(( _t1 - _t0 ))" "$_err"
}

r=$(mode_a poll_until inner_always_fails)
elapsed=${r%%|*}; err=${r#*|}
case "$err" in
    *OUTER-DESC*) ok "MODE A: outer reports its OWN desc on timeout" ;;
    *)            bad "MODE A: outer misreported (got: $err)" ;;
esac
if [ "$elapsed" -ge 5 ]; then ok "MODE A: outer honoured its own 6s budget (${elapsed}s)"
else bad "MODE A: outer aborted early (${elapsed}s, want >=5)"; fi

# non-vacuity: the same case against the pre-R1 impl MUST exhibit the bug
r=$(mode_a broken_poll_until inner_always_fails_broken)
elapsed=${r%%|*}; err=${r#*|}
if [ "$elapsed" -lt 5 ] || ! (printf '%s' "$err" | grep -q OUTER-DESC); then
    ok "MODE A: NON-VACUOUS — pre-R1 impl reproduces the bug (${elapsed}s, err=${err%%$(printf '\n')*})"
else
    bad "MODE A: VACUOUS — pre-R1 impl also passed; this case proves nothing"
fi

# ── MODE B ───────────────────────────────────────────────────────────────────────────────────────────
# Outer asks for 3s and can NEVER succeed. Its predicate nests a poll_until that ALWAYS succeeds with a
# large (30s) budget. Pre-R1 that re-armed the outer deadline every round => unbounded hang. The outer
# must still give up at ~3s. A hard `timeout` bounds the case itself so a regression cannot wedge CI.
inner_always_succeeds()        { poll_until 30 1 "INNER-OK" -- true; return 1; }
inner_always_succeeds_broken() { broken_poll_until 30 1 "INNER-OK" -- true; return 1; }

mode_b() {
    _t0=$(date +%s)
    timeout 20 sh -c "
        . '$HERE/../lib/log.sh'
        $2
        $1 3 1 'OUTER-B' -- ${3}
    " >/dev/null 2>&1
    _rc=$?
    _t1=$(date +%s)
    printf '%s|%s' "$(( _t1 - _t0 ))" "$_rc"
}

# NB: the broken impl must be re-defined inside the subshell, hence passing its text.
BROKEN_TEXT='broken_poll_until() {
    _pu_timeout=$1; _pu_interval=$2; _pu_desc=$3; shift 3
    [ "$1" = "--" ] && shift
    _pu_end=$(( $(date +%s) + _pu_timeout ))
    while :; do
        if "$@" >/dev/null 2>&1; then return 0; fi
        if [ "$(date +%s)" -ge "$_pu_end" ]; then return 1; fi
        sleep "$_pu_interval"
    done
}
inner_always_succeeds_broken() { broken_poll_until 30 1 "INNER-OK" -- true; return 1; }'

FIXED_TEXT='inner_always_succeeds() { poll_until 30 1 "INNER-OK" -- true; return 1; }'

r=$(mode_b poll_until "$FIXED_TEXT" inner_always_succeeds)
elapsed=${r%%|*}; rc=${r#*|}
if [ "$rc" = 124 ]; then
    bad "MODE B: outer HUNG (killed by the 20s backstop) — the unbounded-deadline bug is still present"
elif [ "$elapsed" -le 10 ]; then
    ok "MODE B: outer timed out on its own 3s budget (${elapsed}s, rc=$rc)"
else
    bad "MODE B: outer took ${elapsed}s for a 3s budget"
fi

r=$(mode_b broken_poll_until "$BROKEN_TEXT" inner_always_succeeds_broken)
elapsed=${r%%|*}; rc=${r#*|}
if [ "$rc" = 124 ]; then
    ok "MODE B: NON-VACUOUS — pre-R1 impl hangs until the backstop kills it (${elapsed}s)"
else
    bad "MODE B: VACUOUS — pre-R1 impl also terminated (${elapsed}s, rc=$rc); this case proves nothing"
fi

# ── stack hygiene ────────────────────────────────────────────────────────────────────────────────────
_PU_STACK=''
poll_until 1 1 "hygiene-outer" -- sh -c 'true' >/dev/null 2>&1
if [ -z "$_PU_STACK" ]; then ok "stack hygiene: frame popped on success"
else bad "stack hygiene: leaked frame after success: $_PU_STACK"; fi
poll_until 1 1 "hygiene-outer-fail" -- false >/dev/null 2>&1
if [ -z "$_PU_STACK" ]; then ok "stack hygiene: frame popped on timeout"
else bad "stack hygiene: leaked frame after timeout: $_PU_STACK"; fi

printf -- '--------------------------------------------------------------------------------\n'
if [ "$FAIL" -eq 0 ]; then printf 'poll-reentrancy: ALL PASS (%d)\n' "$PASS"; exit 0; fi
printf 'poll-reentrancy: %d PASS, %d FAIL\n' "$PASS" "$FAIL" >&2; exit 1
