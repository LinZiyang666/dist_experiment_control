# lib/assert.sh — signature-guarded drill harness with a SINGLE machine-readable verdict contract.
# POSIX sh (validated with `sh -n` + `dash -n` + hermetic tests/verdict-contract-test.sh). Sourced by drills.
#
# ─────────────────────────────────────────────────────────────────────────────────────────────────────
# THE VERDICT CONTRACT (SSOT — external review round-3). A drill lands in EXACTLY ONE verdict, computed by
# precedence from four independent, non-negative counters accumulated during the run:
#
#   counter          incremented by                                    → landing verdict   rc
#   ─────────────────────────────────────────────────────────────────────────────────────────────────
#   assert_fail      a KEPT invariant / positive control BROKE;          ASSERT-FAIL        1
#                    an assert_bug that "APPEARS FIXED" (want-fail→got 0);
#                    an assert_bug/assert_refuses signature MISMATCH
#                    (failed for an UNDOCUMENTED reason)
#   setup_red        a prerequisite / fixture / provisioning step failed; SETUP-RED          2
#                    OR a harness MISUSE (missing/empty required arg)
#   product_red      a signature-guarded KNOWN tether defect REPRODUCED    PRODUCT-RED        3
#                    for its documented reason (assert_bug hit the sig)
#   not_covered      a first-class explore→pin coverage gap recorded        INCOMPLETE         4
#                    (a cell genuinely not exercisable in-sim this run)
#   (all zero)       every assertion a KEPT invariant passed, no gaps        GREEN              0
#
#   PRECEDENCE (highest first):  ASSERT-FAIL > SETUP-RED > PRODUCT-RED > INCOMPLETE > GREEN.
#
# NOTE on m1 (round-3): the COUNTER is `not_covered`; the VERDICT it produces is `INCOMPLETE`. Five landing
# verdicts total. `NOT-COVERED` is never a landing verdict — it is the reason line for the INCOMPLETE state.
#
# OWNER / RELEASE POLICY (external review round-4). The mandate is to expose defects without silently
# authorizing them for release. Therefore:
#   • The old "a reproduced known defect counts as overall harness-GREEN (连绿)" discipline is ABOLISHED.
#     A reproduced known defect lands PRODUCT-RED — a DISTINCT, non-green, EXPECTED landing state. It is the
#     harness working as intended (a real tether defect surfaced), but it remains a release blocker by default.
#   • run-drills.sh reports FIVE columns. Every non-GREEN verdict fails closed. An owner may explicitly waive
#     PRODUCT-RED and/or INCOMPLETE for a particular invocation with --allow-product-red/--allow-incomplete;
#     the summary then says WAIVED NON-GREEN, never ALL GREEN or NO BLOCKERS.
#   • A drill whose SPINE is a known defect (e.g. 31-node-upgrade-fleet pins #28) is EXPECTED to land
#     PRODUCT-RED, and the README drill table / roadmap label it PRODUCT-RED, not GREEN. The day the product
#     fix lands, assert_bug sees exit 0 → ASSERT-FAIL "APPEARS FIXED — promote to assert_ok" → then GREEN.
#
# THE STRUCTURED VERDICT LINE (grammar — parsed by run-drills.sh + tests). Machine fields FIRST (all
# space-free key=value), free-text drill name LAST after a ` -- ` sentinel so a description with spaces
# never breaks the parse:
#
#   DRILL-VERDICT verdict=<ENUM> rc=<n> assert_fail=<n> setup_red=<n> product_red=<n> not_covered=<n> pass=<n> -- <drill name>
#
# The line is emitted to STDERR by drill_end, exactly once. A drill that aborts BEFORE drill_end (a real
# infra abort — an unbound var, a `docker` explosion) emits NO such line; the runner classifies that (and
# only that) as INFRA-ABORT. After migration, every prerequisite guard is assert_setup/setup_fail (which
# reach drill_end), so a well-formed drill ALWAYS emits a verdict line.
# ─────────────────────────────────────────────────────────────────────────────────────────────────────
#
# THE API
#   assert_ok      "<desc>" <cmd...>                      → must exit 0 (a KEPT invariant / positive control)
#   assert_refuses "<desc>" "<sig-regex>" <cmd...>        → must FAIL with stderr matching sig (a refusal we KEEP)
#   assert_bug     "<desc>" "<gotcha>" "<sig>" <cmd...>   → a command that SHOULD succeed once fixed:
#        exit 0             → ASSERT-FAIL "APPEARS FIXED — promote to assert_ok" (so we notice the fix)
#        fail & stderr=~sig → PRODUCT-RED  (defect reproduced for the DOCUMENTED reason — expected, tracked)
#        fail & stderr!~sig → ASSERT-FAIL  (broke for an UNDOCUMENTED reason — the false-green guard tripped)
#   assert_setup   "<desc>" <cmd...>                      → a prerequisite step; on failure SETUP-RED + ABORT
#   setup_fail     "<desc>"                               → record SETUP-RED + ABORT (for `X=$(...) || setup_fail`)
#   not_covered    "<desc>" "<reason>"                    → a first-class explore→pin gap → INCOMPLETE (non-green)
#   drill_begin    "<name>" / drill_end                   → frame; drill_end emits the verdict line + returns rc
#
# HARNESS-MISUSE IS FAIL-CLOSED (round-3 M1/m1): a missing/empty <desc>, <sig-regex>, <gotcha>, or <cmd>
# is a SETUP-RED (a HARNESS-ERROR), never a fail-open GREEN and never a crash-without-verdict. An empty
# signature is REJECTED (it would `grep -E ''` match ANY failure and forge a false GREEN / false PRODUCT-RED).

_AS_PASS=0; _AS_FAIL=0; _AS_DRILL="${_AS_DRILL:-drill}"
_AS_NC=0            # not_covered gaps           → INCOMPLETE
_AS_SETUP=0         # setup / prerequisite / misuse failures → SETUP-RED
_AS_PRODUCT_RED=0   # signature-guarded known-defect reproductions → PRODUCT-RED

_as_pass()        { _AS_PASS=$((_AS_PASS+1)); ok   "PASS  $1"; }
_as_fail()        { _AS_FAIL=$((_AS_FAIL+1)); err  "FAIL  $1"; [ -n "${2:-}" ] && printf '        %s\n' "$2" >&2; }
_as_product_red() { _AS_PRODUCT_RED=$((_AS_PRODUCT_RED+1)); err "PRODUCT-RED  $1"; }
_as_setup_red()   { _AS_SETUP=$((_AS_SETUP+1)); err "SETUP-RED  $1"; }

# Harness-misuse recorder: a SETUP-RED that does NOT abort (the caller returns 0 so the drill continues to
# drill_end and still emits a verdict line). Used by the arg validators below.
_as_misuse() { _AS_SETUP=$((_AS_SETUP+1)); err "HARNESS-ERROR  $1"; }

# _as_argcount <need> <have> <api-ctx> : true iff have>=need; else records misuse. Checked BEFORE any
# positional param is read, so `set -u` never trips on a missing $2/$3.
_as_argcount() {
    if [ "$2" -ge "$1" ] 2>/dev/null; then return 0; fi
    _as_misuse "$3 — needs at least $1 args, got $2 (would read an unset positional under set -u)"; return 1
}
# _as_nonempty <api-ctx> <value> : true iff value is non-empty; else records misuse (an empty required arg
# — desc / sig-regex / gotcha / first command word — is a harness bug, never a fail-open success).
_as_nonempty() {
    if [ -n "${2:-}" ]; then return 0; fi
    _as_misuse "$1 — required argument is empty (would fail-open / forge a false verdict)"; return 1
}

_as_capture() { _AS_OUT=$("$@" 2>&1); _AS_RC=$?; return 0; }

assert_ok() {
    _as_argcount 2 "$#" "assert_ok <desc> <cmd...>" || return 0
    _as_desc=$1; shift
    _as_nonempty "assert_ok <desc>" "$_as_desc" || return 0
    _as_nonempty "assert_ok <cmd> (first word)" "${1:-}" || return 0
    _as_capture "$@"
    if [ "$_AS_RC" = 0 ]; then _as_pass "$_as_desc"
    else _as_fail "$_as_desc (want exit 0, got $_AS_RC)" "$(printf '%s' "$_AS_OUT" | tail -3)"; fi
}

assert_refuses() {
    _as_argcount 3 "$#" "assert_refuses <desc> <sig-regex> <cmd...>" || return 0
    _as_desc=$1; _as_sig=$2; shift 2
    _as_nonempty "assert_refuses <desc>" "$_as_desc" || return 0
    _as_nonempty "assert_refuses <sig-regex>" "$_as_sig" || return 0       # empty sig would match ANY failure
    _as_nonempty "assert_refuses <cmd> (first word)" "${1:-}" || return 0
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
    _as_argcount 4 "$#" "assert_bug <desc> <gotcha> <sig> <cmd...>" || return 0
    _as_desc=$1; _as_gotcha=$2; _as_sig=$3; shift 3
    _as_nonempty "assert_bug <desc>" "$_as_desc" || return 0
    _as_nonempty "assert_bug <gotcha>" "$_as_gotcha" || return 0
    _as_nonempty "assert_bug <sig>" "$_as_sig" || return 0                 # empty sig would forge a false PRODUCT-RED
    _as_nonempty "assert_bug <cmd> (first word)" "${1:-}" || return 0
    _as_capture "$@"
    if [ "$_AS_RC" = 0 ]; then
        _as_fail "$_as_desc [$_as_gotcha] APPEARS FIXED — command succeeded; promote to assert_ok + flip to regression" \
                 "$(printf '%s' "$_AS_OUT" | tail -3)"
    elif printf '%s' "$_AS_OUT" | grep -qiE "$_as_sig"; then
        _as_product_red "$_as_desc [$_as_gotcha] reproduced for the documented reason (/$_as_sig/)"
    else
        _as_fail "$_as_desc [$_as_gotcha] failed for an UNDOCUMENTED reason (not /$_as_sig/) — false-green guard tripped" \
                 "$(printf '%s' "$_AS_OUT" | tail -5)"
    fi
}

# out_matches "<sig-regex>" <cmd...> — run <cmd> TO COMPLETION (full output captured; NO pipe), then report
# whether its output matched <sig-regex>. Use this instead of `<cmd> | grep -q <sig>` whenever <cmd> MUTATES
# state.
#
# WHY (round-5 §M1, proven on the real stack): `grep -q` exits at the FIRST match and closes the pipe, which
# SIGPIPEs the writer. A multi-step command can therefore be KILLED MID-OPERATION by the very oracle that is
# measuring it. Drill 91 piped `cluster recovery force-single` into `grep -qiE '…|single-voter|force.single'`;
# that matched ForceSingle's own INTERNAL "…is now a single-voter cluster" log line, which is emitted BEFORE
# the CLI's nats.conf de-cluster step — so grep exited, SIGPIPE killed tether, the de-cluster never ran, and
# the survivor crash-looped (exit 70) forever. The drill then blamed the product for "seeds not converging".
# Three controlled runs isolated the pipe as the sole cause. Never pipe a mutating command into `grep -q`.
# Round-5 (non-blocking finding, adopted): out_matches ALSO requires the command to SUCCEED. The first cut
# only matched the output, so `sh -c 'echo would proceed; exit 9'` passed — any command that prints the
# signature and THEN fails would have been a false pass. Both must hold: rc=0 AND the signature matched.
out_matches() {
    _as_argcount 2 "$#" "out_matches <sig-regex> <cmd...>" || return 1
    _om_sig=$1; shift
    _as_nonempty "out_matches <sig-regex>" "$_om_sig" || return 1
    _as_nonempty "out_matches <cmd> (first word)" "${1:-}" || return 1
    _om_out=$("$@" 2>&1); _om_rc=$?   # runs to completion — no pipe, so no SIGPIPE truncation
    [ "$_om_rc" = 0 ] || { printf '%s\n' "$_om_out" | tail -3 >&2; return 1; }
    printf '%s' "$_om_out" | grep -qiE "$_om_sig"
}

# product_red "<desc>" — record a PRODUCT-RED for a known defect whose signature you have ALREADY captured
# and matched (e.g. you branched on a `grep` of a SLOW / SIDE-EFFECTING command's output — retire, cutover —
# so re-running it via assert_bug is wasteful or unsafe). The <desc> MUST name the matched signature so the
# evidence is auditable. For a FRESH command, prefer assert_bug (it re-runs + guards the signature itself).
product_red() {
    _as_nonempty "product_red <desc>" "${1:-}" || return 0
    _as_product_red "$1"
}

# not_covered "<desc>" "<reason>" — a FIRST-CLASS explore→pin gap → INCOMPLETE (non-green). NOT a PASS.
# Reserve for genuine follow-ups: a topic SPINE (a roadmap locked cell) must be a hard assert or a
# signature-guarded product-RED, NEVER not_covered. Both args are required (an unexplained gap is misuse).
not_covered() {
    _as_argcount 2 "$#" "not_covered <desc> <reason>" || return 0
    _as_nonempty "not_covered <desc>" "${1:-}" || return 0
    _as_nonempty "not_covered <reason>" "${2:-}" || return 0
    _AS_NC=$((_AS_NC+1)); err "NOT-COVERED  $1 — $2"
}

# assert_setup "<desc>" <cmd...> — a PREREQUISITE step. On failure: SETUP-RED + ABORT the drill (the rest
# cannot run without it). Emits a proper SETUP-RED verdict line via drill_end (never a false GREEN + rc1).
assert_setup() {
    _as_argcount 2 "$#" "assert_setup <desc> <cmd...>" || return 0
    _as_desc=$1; shift
    _as_nonempty "assert_setup <desc>" "$_as_desc" || return 0
    _as_nonempty "assert_setup <cmd> (first word)" "${1:-}" || return 0
    _as_capture "$@"
    if [ "$_AS_RC" = 0 ]; then _as_pass "setup: $_as_desc"; return 0; fi
    _AS_SETUP=$((_AS_SETUP+1)); err "SETUP-FAIL  $_as_desc (want exit 0, got $_AS_RC)"
    printf '        %s\n' "$(printf '%s' "$_AS_OUT" | tail -3)" >&2
    drill_end; exit "$?"
}

# setup_fail "<desc>" — record a SETUP-RED and ABORT via drill_end. For `X=$(cmd) || setup_fail "…"` guards
# where the value is captured (so it cannot be wrapped as assert_setup). Emits a SETUP-RED verdict line.
setup_fail() { _AS_SETUP=$((_AS_SETUP+1)); err "SETUP-FAIL  ${1:-unspecified prerequisite}"; drill_end; exit "$?"; }

drill_begin() {
    # M5 safety: drills open with `nuke` + force-single + kills. Refuse to run on anything but a throwaway
    # `drill-*` instance (an unset INSTANCE defaults to the PERSISTENT `sim` cluster). Run drills only via
    # `simcluster drill <name>`, which injects INSTANCE=drill-<name>.
    case "${INSTANCE:-}" in
        drill-*) ;;
        *) printf 'refusing: destructive drill on non-throwaway instance "%s" — run via `simcluster drill <name>`\n' "${INSTANCE:-sim}" >&2; exit 2 ;;
    esac
    _AS_DRILL="${1:-drill}"; _AS_PASS=0; _AS_FAIL=0; _AS_NC=0; _AS_SETUP=0; _AS_PRODUCT_RED=0
    log "=== drill: $_AS_DRILL ==="
}

drill_end() {
    _de_v=GREEN; _de_rc=0
    if   [ "$_AS_FAIL"        != 0 ]; then _de_v=ASSERT-FAIL; _de_rc=1
    elif [ "$_AS_SETUP"       != 0 ]; then _de_v=SETUP-RED;   _de_rc=2
    elif [ "$_AS_PRODUCT_RED" != 0 ]; then _de_v=PRODUCT-RED; _de_rc=3
    elif [ "$_AS_NC"          != 0 ]; then _de_v=INCOMPLETE;  _de_rc=4
    fi
    # the SSOT verdict line the runner + hermetic tests parse (machine fields first, free-text name last):
    printf 'DRILL-VERDICT verdict=%s rc=%s assert_fail=%s setup_red=%s product_red=%s not_covered=%s pass=%s -- %s\n' \
        "$_de_v" "$_de_rc" "$_AS_FAIL" "$_AS_SETUP" "$_AS_PRODUCT_RED" "$_AS_NC" "$_AS_PASS" "$_AS_DRILL" >&2
    if [ "$_de_v" = GREEN ]; then ok "=== $_AS_DRILL: GREEN ($_AS_PASS assertions, 0 gaps) ==="
    else err "=== $_AS_DRILL: $_de_v (pass=$_AS_PASS product-red=$_AS_PRODUCT_RED setup-red=$_AS_SETUP assert-fail=$_AS_FAIL not-covered=$_AS_NC) ==="; fi
    return "$_de_rc"
}
