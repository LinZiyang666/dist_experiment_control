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
# NOT-COVERED CLASSES (batch E). `not_covered` takes a mandatory third arg <class> because the two things
# that land INCOMPLETE have OPPOSITE end-states, and an un-split `not_covered` counter cannot tell the
# programme when it is done:
#
#   class           meaning                                                    terminal requirement
#   ─────────────────────────────────────────────────────────────────────────────────────────────────
#   gap             a REAL coverage hole / defect pin — a cell the drill owes    total nc_gap == 0
#                   but could not exercise, or a finding parked for a dedicated  (the class DISAPPEARS
#                   follow-up. It exists BECAUSE the product (or the drill) is    once the product /
#                   not yet where it should be.                                  drill is fixed)
#   runtime-guard   an HONEST run-time guard — the constructed precondition did   never fires in the
#                   not materialise THIS run, so the arm has nothing to judge     JUDGING run (but the
#                   (e.g. "the loopback tier-B transfer finished before the kill  guard STAYS in the
#                   landed", "a MEASURED process restarted mid-soak so the        code forever)
#                   sample series is broken"). Judging it either way would be a
#                   guess, so the drill declines instead of inventing a verdict.
#
# The distinction is about PERMANENCE, not severity: a `gap` is debt that a fix retires, a `runtime-guard`
# is a permanent honesty valve that a fix can never delete (the non-determinism it guards is intrinsic to
# the sim). Both still increment `not_covered` and both still land INCOMPLETE — a runtime-guard gets NO
# green back door, because a run that could not exercise an arm genuinely did not cover it, whatever the
# reason. What the split buys is a triage answer at end-of-programme: nc_gap>0 means work remains, whereas
# nc_guard>0 means "re-run it; the fixture did not land this time".
# A missing <class>, or a <class> outside {gap, runtime-guard}, is HARNESS MISUSE → SETUP-RED (below).
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
#   DRILL-VERDICT verdict=<ENUM> rc=<n> assert_fail=<n> setup_red=<n> product_red=<n> not_covered=<n> \
#                 nc_gap=<n> nc_guard=<n> pass=<n> -- <drill name>   (one physical line)
#
# `nc_gap` + `nc_guard` are the class breakdown of `not_covered` and ALWAYS sum to it. They were APPENDED
# after the pre-existing `not_covered` field: every older field keeps its name, its type, and its relative
# order, so a parser that reads fields by name is unaffected. A parser that pins the WHOLE line with an
# anchored positional grammar must be taught the two new fields (they sit between not_covered and pass).
#
# The line is emitted to STDERR by drill_end, exactly once. A drill that aborts BEFORE drill_end (a real
# infra abort — an unbound var, a `docker` explosion) emits NO such line; the runner classifies that (and
# only that) as INFRA-ABORT. Every prerequisite guard is assert_setup/setup_fail (which reach drill_end),
# and since R1 `die()` is frame-aware too (see its definition below), so a well-formed drill ALWAYS emits
# a verdict line. Before R1 this sentence was AN OUTRIGHT FALSEHOOD: 38 `die` call sites across six drills
# exited without a verdict, and drill 73 was measured turning a real ASSERT-FAIL into an INFRA-ABORT.
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
#   not_covered    "<desc>" "<reason>" <class>            → a first-class gap → INCOMPLETE (non-green);
#                                                           <class> ∈ {gap, runtime-guard} (see above)
#   drill_begin    "<name>" / drill_end                   → frame; drill_end emits the verdict line + returns rc
#
# HARNESS-MISUSE IS FAIL-CLOSED (round-3 M1/m1): a missing/empty <desc>, <sig-regex>, <gotcha>, <cmd>, or
# not_covered <class> is a SETUP-RED (a HARNESS-ERROR), never a fail-open GREEN and never a
# crash-without-verdict. An empty signature is REJECTED (it would `grep -E ''` match ANY failure and forge a
# false GREEN / false PRODUCT-RED). An unclassified/mis-classified gap is REJECTED for the same family of
# reason: it silently pollutes the end-of-programme "is the debt retired?" answer with un-triageable rows.

_AS_PASS=0; _AS_FAIL=0; _AS_DRILL="${_AS_DRILL:-drill}"
_AS_NC=0            # not_covered gaps           → INCOMPLETE
_AS_NC_GAP=0        # …of which class=gap           (real debt; must reach 0 when the product/drill is fixed)
_AS_NC_GUARD=0      # …of which class=runtime-guard (honest guards; must not FIRE in a judging run)
_AS_SETUP=0         # setup / prerequisite / misuse failures → SETUP-RED
_AS_PRODUCT_RED=0   # signature-guarded known-defect reproductions → PRODUCT-RED

# ── THE FLIGHT RECORDER (M1) ────────────────────────────────────────────────────────────────────────
# Every non-green assertion writes a full evidence record to $SIM_EVIDENCE_DIR/<drill>.evidence.
#
# WHY. On 2026-07-23 five drills deviated from their recorded expectation. Attributing them cost an
# evening of solo re-runs, because the console keeps only `tail -3` of the failing command's output —
# and for drills 20/91 the three lines kept were the DATA-IMPACT warning, while the line that actually
# named the cause ("re-run this exact force-single command with --reset-js") was truncated away. The
# same truncation hid drill 52's `error: not leader`. The fix is not more re-runs: it is to record, at
# the moment of failure and on the FIRST run, everything a human would go back for.
#
# THREE RULES, all load-bearing:
#   1. CAPTURE NEVER CHANGES A VERDICT. No counter is touched, no rc is propagated, every probe is
#      bounded by `timeout` and swallowed with `|| true`. If $SIM_EVIDENCE_DIR is unwritable the drill
#      must emit a BYTE-IDENTICAL verdict line — pinned by tests/verdict-contract-test.sh.
#   2. THE CONSOLE IS UNCHANGED. `tail -3` still governs what a human sees scrolling past; the full
#      output goes to the file. Widening the console would bury the signal it exists to show.
#   3. IT IS APPENDED, NEVER GRAFTED ONTO THE CONTRACT. drill_end emits one extra DRILL-EVIDENCE line.
#      The DRILL-VERDICT grammar is untouched — three parsers depend on it.
_AS_ORD=0                # assertion ordinal within this drill (every assertion, pass or not)
_AS_FIRST_FAIL_ORD=0     # ordinal of the FIRST non-green assertion — the one worth reading first
_AS_EV_HOST_DONE=0       # host-level probes run once per drill, not once per failure

# The evidence filename MUST agree with what run-drills.sh's deviation report opens, which is keyed on the
# drill FILENAME ($d), not the free-text drill_begin title. Internal review B1: naming it from $_AS_DRILL
# (the title — "#20/#12 OFFLINE force-single …") produced a file the reader never found for ANY of the 38
# drills, so the report silently fell back to `tail -5` of the .log — the exact truncation M1 exists to
# kill. run_one exports SIM_DRILL_ID (the filename); prefer it, and fall back to the sanitized title only
# for the hermetic tests, which run drill bodies directly and resolve the path from the emitted
# DRILL-EVIDENCE line rather than by guessing it.
_as_ev_file() {
    _ev_dir="${SIM_EVIDENCE_DIR:-${TMPDIR:-/tmp}}"
    if [ -n "${SIM_DRILL_ID:-}" ]; then
        printf '%s/%s.evidence' "$_ev_dir" "$SIM_DRILL_ID"
    else
        printf '%s/%s.evidence' "$_ev_dir" "$(printf '%s' "${_AS_DRILL:-drill}" | tr -cs 'A-Za-z0-9._-' '-')"
    fi
}

# _as_redact : mask secret-bearing values on stdin (external review Medium 6). Covers `--pin V`,
# `--pin=V`, `--token/--secret/--password V`, `PIN=V`/`TOKEN=`/`PASSWORD=` env forms, and the pty-confirm
# 6-digit PINs. Best-effort — it protects the common shapes the drills actually use, not an exhaustive
# scanner. Runs under `LC_ALL=C` for portable sed.
_as_redact() {
    LC_ALL=C sed -E \
        -e 's/(--(pin|token|secret|password|pass)[= ])[^ ]+/\1<REDACTED>/g' \
        -e 's/((PIN|TOKEN|SECRET|PASSWORD|PASS)=)[^ ]+/\1<REDACTED>/g' \
        -e 's/([?&](pin|token|secret|password|pass)=)[^ &]+/\1<REDACTED>/g' \
        2>/dev/null || cat
}

# Render argv without losing argument boundaries and redact secrets BEFORE the string is persisted. `$*`
# cannot represent boundaries: an argument `--pin "two words"` becomes indistinguishable from two public
# arguments, so a later regex either leaks `words` or must erase the rest of the command. This formatter
# walks the original argv, masks the value following a secret flag (including spaces/quotes/newlines), and
# then emits shell-readable single-quoted fields. URI/KEY=value forms are redacted per argument as well.
_as_quote_arg() {
    case "$1" in
        ''|*[!A-Za-z0-9_./:@%+=,-]*)
            printf "'"
            printf '%s' "$1" | sed "s/'/'\\\\''/g"
            printf "'"
            ;;
        *) printf '%s' "$1" ;;
    esac
}
_as_format_argv() {
    _afa_need_secret=0
    _afa_sep=""
    for _afa_arg in "$@"; do
        if [ "$_afa_need_secret" = 1 ]; then
            _afa_safe="<REDACTED>"
            _afa_need_secret=0
        else
            case "$_afa_arg" in
                --pin|--token|--secret|--password|--pass)
                    _afa_safe="$_afa_arg"
                    _afa_need_secret=1
                    ;;
                --pin=*|--token=*|--secret=*|--password=*|--pass=*|\
                PIN=*|TOKEN=*|SECRET=*|PASSWORD=*|PASS=*|\
                pin=*|token=*|secret=*|password=*|pass=*)
                    _afa_safe="${_afa_arg%%=*}=<REDACTED>"
                    ;;
                *\?pin=*|*\&pin=*|*\?token=*|*\&token=*|*\?secret=*|*\&secret=*|\
                *\?password=*|*\&password=*|*\?pass=*|*\&pass=*)
                    # sed is line-oriented; a query value containing a newline would otherwise leak its
                    # continuation. Mask the whole URI argument rather than pretend we can preserve its
                    # other query fields safely without a real URI parser.
                    _afa_safe="<REDACTED-URI>"
                    ;;
                *) _afa_safe=$(printf '%s' "$_afa_arg" | _as_redact) ;;
            esac
        fi
        printf '%s' "$_afa_sep"
        _as_quote_arg "$_afa_safe"
        _afa_sep=" "
    done
}

# _as_evidence <kind> <desc> : append one record. Best effort, always returns 0.
_as_evidence() {
    _ev_kind=$1; _ev_desc=$2
    _ev_f=$(_as_ev_file)
    _ev_dir=${_ev_f%/*}
    [ -d "$_ev_dir" ] || mkdir -p "$_ev_dir" 2>/dev/null || return 0
    { : >>"$_ev_f"; } 2>/dev/null || return 0     # unwritable target: give up SILENTLY, never fail a drill
    [ "$_AS_FIRST_FAIL_ORD" = 0 ] && _AS_FIRST_FAIL_ORD=$_AS_ORD
    {
        printf '\n=== %s ord=%s kind=%s drill=%s\n' \
            "$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo unknown)" "$_AS_ORD" "$_ev_kind" "${_AS_DRILL:-drill}"
        printf 'desc: %s\n' "$_ev_desc"
        # argv/rc/took belong to the captured command and are valid ONLY when the capture is this
        # ordinal's (MA1) — otherwise they are the previous, passing command's, which is worse than blank.
        if [ "${_AS_CAP_ORD:-0}" = "$_AS_ORD" ]; then
            # External review Medium 6: REDACT secret-bearing values before persisting. argv/output carry
            # session PINs, invite tokens, and confirm values; $LOGDIR is now 0700, but redaction is
            # defence-in-depth so an archived/copied evidence file is not a credential dump.
            [ -n "${_AS_ARGV:-}" ] && printf 'argv: %s\n' "$(printf '%s' "$_AS_ARGV" | _as_redact)"
            [ -n "${_AS_RC:-}" ]   && printf 'rc:   %s\n' "$_AS_RC"
            [ -n "${_AS_MS:-}" ]   && printf 'took: %s ms\n' "$_AS_MS"
        fi
        # THE FULL output — this is the whole point; the console's tail -3 is what lost drill 20's cause.
        # Internal review MA1: only emit the captured block when the capture belongs to THIS ordinal.
        # _as_capture stamps _AS_CAP_ORD with the ordinal it is about to feed; a non-capturing red
        # (a bare product_red, setup_fail, not_covered) leaves the PREVIOUS capture in the variables, so
        # without this guard a PRODUCT-RED after a passing assert_ok would record "rc: 0" and the earlier,
        # unrelated command's output. When the stamp does not match, fall through to the EMPTY note.
        if [ -n "${_AS_OUT:-}" ] && [ "${_AS_CAP_ORD:-0}" = "$_AS_ORD" ]; then
            printf -- '--- stdout+stderr (FULL, %s line(s)) ---\n' "$(printf '%s\n' "$_AS_OUT" | wc -l | tr -d ' ')"
            printf '%s\n' "$_AS_OUT" | _as_redact      # Medium 6: mask secrets in the persisted output
            printf -- '--- end ---\n'
        elif [ "${_AS_CAP_ORD:-0}" != "$_AS_ORD" ]; then
            printf -- '--- stdout+stderr: NOT CAPTURED for this assertion ---\n'
            printf 'NOTE: this red was recorded by a non-capturing path (product_red / setup_fail /\n'
            printf '      not_covered) that ran no command through the harness, so there is no argv/rc/\n'
            printf '      output to show. The evidence, if any, is in the console log around this ordinal.\n'
            printf -- '--- end ---\n'
        elif [ -n "${_AS_ARGV:-}" ]; then
            # NOT a harmless blank. A predicate that captures its command's output into a local variable,
            # greps it, and discards it leaves _AS_OUT EMPTY — so this recorder, and the console, and the
            # deviation report all have nothing to show but "(want exit 0, got 1)". That is precisely how
            # drill 20's `--reset-js` refusal became invisible on 2026-07-23 while drill 91, which let the
            # same refusal reach the harness, printed it. Say so out loud instead of leaving a silent gap:
            # the blindness is a DRILL defect, and an operator staring at an empty record must be told
            # where to look rather than left to conclude the command said nothing.
            printf -- '--- stdout+stderr: EMPTY ---\n'
            printf 'NOTE: the failing predicate produced no output for the harness to record. If it is a\n'
            printf '      function that does `v=$(cmd 2>&1); printf %%s "$v" | grep -q ...`, the evidence was\n'
            printf '      swallowed INSIDE the drill, not truncated here — make it re-emit $v on failure.\n'
            printf -- '--- end ---\n'
        fi
        if [ "$_AS_EV_HOST_DONE" = 0 ]; then
            _AS_EV_HOST_DONE=1
            printf -- '--- host at first failure ---\n'
            printf 'loadavg: %s\n' "$(timeout 10 cat /proc/loadavg 2>/dev/null || true)"
            # fsync latency is the ONE coupling docker does not isolate. On this host the p50 is
            # DEVICE-BOUND at ~6.4 ms whether idle or loaded (a consumer NVMe's flush cost); only the tail
            # (p90/p99) widens under contention. Recording it AT the failure is what lets a later
            # disposition say "this red happened on a loaded disk" with evidence rather than a hunch.
            # Internal review MA4: the previous parser printed dd's LAST TWO fields — the THROUGHPUT pair
            # ("683 kB/s"), under a name that says milliseconds. Parse the SECONDS field (the value before
            # the standalone "s" token) and convert to ms.
            printf 'fsync_4k_ms: %s\n' "$(timeout 10 sh -c 'LC_ALL=C dd if=/dev/zero of="'"$_ev_dir"'/.fsprobe.$$" bs=4096 count=1 conv=fdatasync 2>&1 | awk "/copied/{gsub(/,/,\" \"); for(i=1;i<=NF;i++) if(\$i==\"s\"){printf \"%.3f\", \$(i-1)*1000; exit}}"; rm -f "'"$_ev_dir"'/.fsprobe.$$"' 2>/dev/null || true)"
            printf 'disk_free: %s\n' "$(timeout 10 df -Pk "$_ev_dir" 2>/dev/null | awk 'NR==2{print $4" KiB free on "$1}' || true)"
            printf 'containers:\n%s\n' "$(timeout 10 docker ps --filter "label=sim.instance=${INSTANCE:-}" --format '  {{.Names}} {{.Status}}' 2>/dev/null || true)"
        fi
    } >>"$_ev_f" 2>/dev/null || true
    return 0
}

_as_pass()        { _AS_ORD=$((_AS_ORD+1)); _AS_PASS=$((_AS_PASS+1)); ok   "PASS  $1"; }
_as_fail()        { _AS_ORD=$((_AS_ORD+1)); _AS_FAIL=$((_AS_FAIL+1)); err  "FAIL  $1"; [ -n "${2:-}" ] && printf '        %s\n' "$2" >&2; _as_evidence ASSERT-FAIL "$1"; }
_as_product_red() { _AS_ORD=$((_AS_ORD+1)); _AS_PRODUCT_RED=$((_AS_PRODUCT_RED+1)); err "PRODUCT-RED  $1"; _as_evidence PRODUCT-RED "$1"; }
# Console label is SETUP-FAIL, matching assert_setup/setup_fail and the archived logs — the deviation
# report and attribution greps anchor on `^\[err \] (FAIL|SETUP-FAIL|…)`, so a SETUP-RED string here would
# make every SETUP-RED first-failure invisible to them. The evidence-record KIND stays SETUP-RED.
_as_setup_red()   { _AS_ORD=$((_AS_ORD+1)); _AS_SETUP=$((_AS_SETUP+1)); err "SETUP-FAIL  $1"; _as_evidence SETUP-RED "$1"; }

# Harness-misuse recorder: a SETUP-RED that does NOT abort (the caller returns 0 so the drill continues to
# drill_end and still emits a verdict line). Used by the arg validators below.
_as_misuse() { _AS_ORD=$((_AS_ORD+1)); _AS_SETUP=$((_AS_SETUP+1)); err "HARNESS-ERROR  $1"; _as_evidence HARNESS-ERROR "$1"; }

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

# Milliseconds since the epoch. `date +%s%N` is GNU/Linux (the only platform drills run on — they are
# docker containers); the fallback keeps this from exploding into a syntax error anywhere else, at the
# cost of second granularity. Timing is DIAGNOSTIC ONLY and never gates a verdict.
_as_now_ms() {
    _nm=$(date +%s%N 2>/dev/null) || _nm=""
    case "$_nm" in
        ''|*[!0-9]*|*N*) printf '%s000' "$(date +%s)" ;;
        *) printf '%s' "$(( _nm / 1000000 ))" ;;
    esac
}

# _as_capture records the FULL output, the rc, the argv and the wall time of the command under test.
# argv + elapsed exist for the flight recorder: "which command, and was it slow?" is the first pair of
# questions any attribution asks, and neither was recoverable from a drill log before M1.
_as_capture() {
    _AS_ARGV=$(_as_format_argv "$@")
    # Stamp the ordinal this capture is FOR (the next assertion to be recorded). _as_evidence emits the
    # argv/rc/output block only when this equals the ordinal being recorded, so a later non-capturing red
    # cannot inherit this command's data (MA1). The +1 is because _as_pass/_as_fail increment _AS_ORD
    # AFTER capture, so this capture belongs to the upcoming ordinal.
    _AS_CAP_ORD=$((_AS_ORD+1))
    _as_t0=$(_as_now_ms)
    _AS_OUT=$("$@" 2>&1); _AS_RC=$?
    _AS_MS=$(( $(_as_now_ms) - _as_t0 ))
    return 0
}

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

# not_covered "<desc>" "<reason>" <class> — a FIRST-CLASS gap → INCOMPLETE (non-green). NOT a PASS.
# Reserve for genuine follow-ups: a topic SPINE (a roadmap locked cell) must be a hard assert or a
# signature-guarded product-RED, NEVER not_covered. All THREE args are required — an unexplained gap is
# misuse, and so is an UNCLASSIFIED one (see the NOT-COVERED CLASSES block in the header):
#   gap           → real debt; the terminal requirement is nc_gap == 0 (a fix makes it disappear).
#   runtime-guard → an honest "the fixture did not materialise this run" valve; it stays in the code
#                   forever, so its terminal requirement is that it does not FIRE in the judging run.
# Both classes increment the SAME `not_covered` counter and both land INCOMPLETE — runtime-guard gets no
# green back door, because a run that could not exercise the arm did not cover it whatever the reason.
not_covered() {
    _as_argcount 3 "$#" "not_covered <desc> <reason> <class>" || return 0
    _as_nonempty "not_covered <desc>" "${1:-}" || return 0
    _as_nonempty "not_covered <reason>" "${2:-}" || return 0
    _as_nonempty "not_covered <class>" "${3:-}" || return 0
    # Classify BEFORE incrementing: an unknown class must not slip into the totals as a nameless gap.
    case "$3" in
        gap)           _AS_NC_GAP=$((_AS_NC_GAP+1)) ;;
        runtime-guard) _AS_NC_GUARD=$((_AS_NC_GUARD+1)) ;;
        *) _as_misuse "not_covered <class> — must be exactly 'gap' or 'runtime-guard', got '$3' (an untriageable gap would corrupt the end-of-programme debt count)"; return 0 ;;
    esac
    # The ordinal counts every assertion-shaped event so "first_fail_ord=N" locates the failure in the
    # drill's own PASS/NOT-COVERED sequence. A coverage gap is deliberately NOT given an evidence record:
    # it is a declared hole, not a failure, and recording host telemetry for it would dilute the file
    # that exists to explain reds.
    _AS_ORD=$((_AS_ORD+1))
    _AS_NC=$((_AS_NC+1)); err "NOT-COVERED[$3]  $1 — $2"
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
    # Internal review B2: this used to bump _AS_SETUP inline and never record evidence, so the flight
    # recorder was BLIND on every SETUP-RED — including drills 30 and 91, the two SETUP-RED deviations M1
    # was built to attribute. Route through _as_setup_red, which advances the ordinal and writes the
    # evidence record (the capture above means its argv/rc/output are this ordinal's, MA1-clean).
    _as_setup_red "$_as_desc (want exit 0, got $_AS_RC)"
    printf '        %s\n' "$(printf '%s' "$_AS_OUT" | tail -3)" >&2
    drill_end; exit "$?"
}

# setup_fail "<desc>" — record a SETUP-RED and ABORT via drill_end. For `X=$(cmd) || setup_fail "…"` guards
# where the value is captured (so it cannot be wrapped as assert_setup). Emits a SETUP-RED verdict line.
# B2: also routes through _as_setup_red so the abort is recorded. There is no captured command here, so
# the evidence record correctly shows the NOT-CAPTURED note rather than a stale prior command (MA1).
setup_fail() { _as_setup_red "${1:-unspecified prerequisite}"; drill_end; exit "$?"; }

# R1: MAKE THE VERDICT CONTRACT TOTAL. The header above claims "a well-formed drill ALWAYS emits a verdict
# line". That claim was FALSE: lib/log.sh's die() is `err "$*"; exit 1`, and 38 call sites across six drills
# (73×20, 74×8, 71×5, 32×2, 70×2, 31×1) use it as a mid-drill refusal guard — every one of them exits
# WITHOUT reaching drill_end, so the runner sees no verdict line and reports INFRA-ABORT. Measured live:
# drill 73's `_still_homed_on … || die "vacuous kill; refusing"` turned a drill that had ALREADY recorded a
# genuine assert_fail (its REHOME-precond assertion failed on the line above) into an INFRA-ABORT — the one
# classification the runner treats as "the harness broke", i.e. the real finding was relabelled as our bug.
#
# Rewriting 38 call sites would be 38 chances to change a judgement. Instead the FRAME owns the exit: once
# drill_begin has run, die() records SETUP-RED and leaves through drill_end, so precedence still applies
# (an already-recorded assert_fail keeps ASSERT-FAIL; a bare refusal lands SETUP-RED). Outside a frame —
# lib/*.sh helpers sourced by simcluster itself — die keeps its original abort semantics.
die() {
    if [ "${_AS_FRAME_OPEN:-0}" = 1 ]; then setup_fail "$*"; fi
    err "$*"; exit 1
}

drill_begin() {
    # M5 safety: drills open with `nuke` + force-single + kills. Refuse to run on anything but a throwaway
    # `drill-*` instance (an unset INSTANCE defaults to the PERSISTENT `sim` cluster). Run drills only via
    # `simcluster drill <name>`, which injects INSTANCE=drill-<name>.
    case "${INSTANCE:-}" in
        drill-*) ;;
        *) printf 'refusing: destructive drill on non-throwaway instance "%s" — run via `simcluster drill <name>`\n' "${INSTANCE:-sim}" >&2; exit 2 ;;
    esac
    _AS_DRILL="${1:-drill}"; _AS_PASS=0; _AS_FAIL=0; _AS_NC=0; _AS_SETUP=0; _AS_PRODUCT_RED=0
    _AS_NC_GAP=0; _AS_NC_GUARD=0
    _AS_ORD=0; _AS_FIRST_FAIL_ORD=0; _AS_EV_HOST_DONE=0
    _AS_ARGV=""; _AS_RC=""; _AS_MS=""; _AS_OUT=""
    _AS_FRAME_OPEN=1          # R1: arms the contract-total die() below
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
    printf 'DRILL-VERDICT verdict=%s rc=%s assert_fail=%s setup_red=%s product_red=%s not_covered=%s nc_gap=%s nc_guard=%s pass=%s -- %s\n' \
        "$_de_v" "$_de_rc" "$_AS_FAIL" "$_AS_SETUP" "$_AS_PRODUCT_RED" "$_AS_NC" "$_AS_NC_GAP" "$_AS_NC_GUARD" "$_AS_PASS" "$_AS_DRILL" >&2
    # M1: an APPENDED line, deliberately NOT part of the DRILL-VERDICT grammar (three parsers pin that
    # line and a sixth field would break all of them). Emitted only when a record was actually written,
    # so an unwritable evidence dir leaves the drill's output byte-identical to the pre-M1 form.
    if [ "${_AS_FIRST_FAIL_ORD:-0}" != 0 ] && [ -s "$(_as_ev_file)" ]; then
        printf 'DRILL-EVIDENCE file=%s first_fail_ord=%s\n' "$(_as_ev_file)" "$_AS_FIRST_FAIL_ORD" >&2
    fi
    # M3: how much of this drill was spent waiting on the cluster, as opposed to doing anything. Also an
    # appended line, for the same reason as DRILL-EVIDENCE: the DRILL-VERDICT grammar is parsed in three
    # places and must not grow fields.
    if command -v poll_wait_total >/dev/null 2>&1; then
        # "direct" because it covers only top-level poll_until calls, not those nested inside an assert
        # predicate's subshell (see poll_wait_total's LIMIT note). Naming it honestly stops a later phase
        # from tuning against it as if it were the complete wait budget.
        printf 'DRILL-POLL-WAIT direct_total=%ss\n' "$(poll_wait_total)" >&2
    fi
    if [ "$_de_v" = GREEN ]; then ok "=== $_AS_DRILL: GREEN ($_AS_PASS assertions, 0 gaps) ==="
    else err "=== $_AS_DRILL: $_de_v (pass=$_AS_PASS product-red=$_AS_PRODUCT_RED setup-red=$_AS_SETUP assert-fail=$_AS_FAIL not-covered=$_AS_NC [gap=$_AS_NC_GAP guard=$_AS_NC_GUARD]) ==="; fi
    return "$_de_rc"
}

# drill_install_traps <cleanup-fn> : install cancellation-safe traps.
#
# EXT-REVIEW-B5. The old `trap '_cleanup' EXIT INT TERM` is WRONG for INT/TERM: a POSIX signal handler
# RETURNS to the command after the interrupted one, so a Ctrl-C mid-drill runs the cleanup and then
# RESUMES executing kills / iptables / cert overwrites / volume disasters — and races cmd_drill's own
# nuke. Register EXIT for the normal path; register INT/TERM handlers that clean up, DISARM the EXIT trap
# (so cleanup never runs twice), and exit 128+signo WITHOUT resuming. Every drill must call this instead
# of a combined `EXIT INT TERM` trap (enforced by tests/lint-drills.sh rule `combined-signal-trap`).
drill_install_traps() {
    _dit_fn=$1
    trap "$_dit_fn" EXIT
    trap "$_dit_fn; trap - EXIT; exit 130" INT
    trap "$_dit_fn; trap - EXIT; exit 143" TERM
}
