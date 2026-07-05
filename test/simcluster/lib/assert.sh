# lib/assert.sh — signature-guarded RED/GREEN drill harness. POSIX-ish sh. Sourced by drills.
#
# The ONE load-bearing idea (plan §5, fidelity critic F9): a drill must NOT score a bug "reproduced"
# unless it failed for the DOCUMENTED cause. #20/#12 have no green code path today (open backlog),
# so we assert the CURRENT broken behavior as RED, guarded by a cause-token signature, and flip to a
# plain GREEN regression the day the product fix lands. No XFAIL/XPASS/promotion framework.
#
#   assert_ok      "<desc>" <cmd...>                 → must exit 0 (a GREEN invariant / positive control)
#   assert_refuses "<desc>" "<sig-regex>" <cmd...>   → must FAIL with stderr matching sig (a refusal we KEEP)
#   assert_bug     "<desc>" "<gotcha>" "<sig>" <cmd...> → the command that SHOULD succeed once fixed:
#        exit 0             → loud "APPEARS FIXED — promote to assert_ok"; drill FAILS (so we notice)
#        fail & stderr=~sig → bug reproduced for the DOCUMENTED reason (expected; drill stays green)
#        fail & stderr!~sig → HARD FAIL (broke for an undocumented reason — e.g. the alert gate, not JS)

_AS_PASS=0; _AS_FAIL=0; _AS_DRILL="${_AS_DRILL:-drill}"

_as_capture() { # runs "$@", stores rc in _AS_RC and combined output in _AS_OUT
    _AS_OUT=$("$@" 2>&1); _AS_RC=$?; return 0
}
_as_pass() { _AS_PASS=$((_AS_PASS+1)); ok   "PASS  $1"; }
_as_fail() { _AS_FAIL=$((_AS_FAIL+1)); err  "FAIL  $1"; [ -n "${2:-}" ] && printf '        %s\n' "$2" >&2; }

assert_ok() {
    _as_desc=$1; shift
    _as_capture "$@"
    if [ "$_AS_RC" = 0 ]; then _as_pass "$_as_desc"
    else _as_fail "$_as_desc (want exit 0, got $_AS_RC)" "$(printf '%s' "$_AS_OUT" | tail -3)"; fi
}

assert_refuses() {
    _as_desc=$1; _as_sig=$2; shift 2
    _as_capture "$@"
    if [ "$_AS_RC" = 0 ]; then
        _as_fail "$_as_desc (expected a refusal, but it SUCCEEDED)" "$(printf '%s' "$_AS_OUT" | tail -3)"
    elif printf '%s' "$_AS_OUT" | grep -qiE "$_as_sig"; then
        _as_pass "$_as_desc (refused as expected: /$_as_sig/)"
    else
        _as_fail "$_as_desc (refused, but NOT for /$_as_sig/)" "$(printf '%s' "$_AS_OUT" | tail -3)"
    fi
}

assert_bug() {
    _as_desc=$1; _as_gotcha=$2; _as_sig=$3; shift 3
    _as_capture "$@"
    if [ "$_AS_RC" = 0 ]; then
        _as_fail "$_as_desc [$_as_gotcha] APPEARS FIXED — command succeeded; promote to assert_ok + flip to regression" \
                 "$(printf '%s' "$_AS_OUT" | tail -3)"
    elif printf '%s' "$_AS_OUT" | grep -qiE "$_as_sig"; then
        _as_pass "$_as_desc [$_as_gotcha] reproduced for the documented reason (/$_as_sig/)"
    else
        _as_fail "$_as_desc [$_as_gotcha] failed for an UNDOCUMENTED reason (not /$_as_sig/) — false-green guard tripped" \
                 "$(printf '%s' "$_AS_OUT" | tail -5)"
    fi
}

drill_begin() {
    # M5 safety: drills open with `nuke` + force-single + kills. Refuse to run on anything but a
    # throwaway `drill-*` instance (an unset INSTANCE defaults to the PERSISTENT `sim` cluster). Run
    # drills only via `simcluster drill <name>`, which injects INSTANCE=drill-<name>.
    case "${INSTANCE:-}" in
        drill-*) ;;
        *) printf 'refusing: destructive drill on non-throwaway instance "%s" — run via `simcluster drill <name>`\n' "${INSTANCE:-sim}" >&2; exit 2 ;;
    esac
    _AS_DRILL=$1; _AS_PASS=0; _AS_FAIL=0; log "=== drill: $_AS_DRILL ==="
}
drill_end() {
    if [ "$_AS_FAIL" = 0 ]; then ok  "=== $_AS_DRILL: GREEN ($_AS_PASS assertions) ==="; return 0
    else err "=== $_AS_DRILL: RED ($_AS_FAIL failed, $_AS_PASS passed) ==="; return 1; fi
}
