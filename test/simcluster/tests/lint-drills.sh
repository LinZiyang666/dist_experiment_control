#!/bin/sh
# lint-drills.sh — STATIC guard for the drill verdict contract (external review round-3 B2). Forbids the
# false-green anti-patterns the S6-S8 migration removed, so they can never creep back into the contract-
# enforced batch:
#
#   1. the old setup-abort:  `err "setup…"; drill_end; exit 1`  → must be assert_setup / setup_fail
#   2. a bare NOT-COVERED note via warn/log (a topic gap reported but NOT counted) → must be not_covered()
#   3. `; true"` trailing an ASSERT command (masks the real exit code = a false GREEN)  [asserts only, not
#      fire-and-forget setup/cleanup, which legitimately end `…; true`]
#   4. a manual counter poke `_AS_FAIL=…` / `_AS_NC=…` in a drill → must use the public API
#   5. every drill must open with drill_begin and close with drill_end (else no verdict line is emitted)
#
# SCOPE. The 9 S6-S8 drills below are CONTRACT-ENFORCED: any violation is a HARD failure (exit 1). The
# runner (run-drills.sh) additionally cross-checks every drill's verdict-line rc against its process rc, so a
# LEGACY drill from an earlier batch that still uses `…; drill_end; exit N` is caught as VERDICT-RC-MISMATCH
# at RUN time regardless of this lint. Pass --all to also print an ADVISORY report of legacy-drill debt (a
# tracked follow-up: migrate earlier batches to the verdict contract); legacy findings never fail this lint.
#
# Run:  sh tests/lint-drills.sh          (hard-gate the 9 S6-S8 drills)
#       sh tests/lint-drills.sh --all    (+ advisory legacy-debt report)
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
DRILLS_DIR="$(cd "$HERE/../drills" && pwd)"
BATCH="22-forcesingle-online 40-drain-retire 41-shrink-to-standalone 42-rejoin-returning 43-migrate-live-data 90-alerts-lifecycle 91-client-converge 92-js503-remote-alert 93-metrics-observability"
ALL=0; [ "${1:-}" = --all ] && ALL=1

# strip full-line comments so the bans apply to executed code, not documentation.
code() { grep -vE '^[[:space:]]*#' "$1"; }

# scan_file <path> : prints one line per violation ("<kind>: <detail>"); prints nothing if clean.
scan_file() {
    _f=$1; _C=$(code "$_f")
    printf '%s\n' "$_C" | grep -qE 'drill_end[[:space:]]*;[[:space:]]*exit[[:space:]]+[0-9]' \
        && echo "setup-abort: 'drill_end; exit N' — use setup_fail/assert_setup (SETUP-RED), not a manual abort"
    printf '%s\n' "$_C" | grep -qE '^[[:space:]]*(warn|log)[[:space:]].*NOT-COVERED' \
        && echo "bare-not-covered: a NOT-COVERED note via warn/log — record it with not_covered() so it counts toward INCOMPLETE"
    printf '%s\n' "$_C" | grep -qE 'assert_[a-z]+.*;[[:space:]]*true"' \
        && echo "true-masking: a trailing '; true\"' inside an assert — masks the exit code (false GREEN)"
    printf '%s\n' "$_C" | grep -qE '_AS_(FAIL|NC|SETUP|PRODUCT_RED)=' \
        && echo "counter-poke: a manual _AS_* counter poke — use the public API (assert_*/product_red/not_covered/setup_fail)"

    # round-5 §M1: piping a MUTATING, multi-step tether command into `grep -q` lets grep's first-match exit
    # SIGPIPE-kill the command MID-OPERATION. Proven: drill 91 truncated `force-single` before its nats.conf
    # de-cluster step and then blamed the product. Use out_matches (captures to completion) or assert the
    # real post-condition instead.
    # The left side must be a REAL `tether …` invocation — grepping an already-captured variable
    # (`printf '%s' "$out" | grep -q …`) is safe and must not be flagged.
    printf '%s\n' "$_C" | grep -qE 'tether[^|]*(force-single|cluster retire|cluster add|cluster init|reconcile nats|rejoin prepare|resnapshot|cluster upgrade|node upgrade|cluster drain|rebalance)[^|]*\|[[:space:]]*grep -q' \
        && echo "sigpipe-truncation: a MUTATING tether command piped into 'grep -q' — grep's first-match exit SIGPIPEs it mid-operation (round-5 §M1); use out_matches or assert the post-condition"
    printf '%s\n' "$_C" | grep -qE '(^|[^_])drill_begin[[:space:]]' || echo "no-frame: missing drill_begin"
    printf '%s\n' "$_C" | grep -qE '(^|[^_])drill_end'             || echo "no-frame: missing drill_end (no verdict line)"
}

is_batch() { case " $BATCH " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

HARD=0; LEGACY=0
echo "── contract-enforced batch (S6-S8) ────────────────────────────────────────────"
for name in $BATCH; do
    f="$DRILLS_DIR/$name.sh"
    [ -f "$f" ] || { echo "  MISSING  $name.sh"; HARD=$((HARD+1)); continue; }
    out=$(scan_file "$f")
    if [ -n "$out" ]; then printf '%s\n' "$out" | while IFS= read -r v; do echo "  VIOLATION $name: $v"; done; HARD=$((HARD+$(printf '%s\n' "$out" | grep -c .)));
    else echo "  ok   $name"; fi
done

if [ "$ALL" = 1 ]; then
    echo "── legacy drills (advisory — migration follow-up, does NOT fail this lint) ──────"
    for f in "$DRILLS_DIR"/*.sh; do
        name=$(basename "$f" .sh); is_batch "$name" && continue
        out=$(scan_file "$f")
        if [ -n "$out" ]; then printf '%s\n' "$out" | while IFS= read -r v; do echo "  advisory  $name: $v"; done; LEGACY=$((LEGACY+$(printf '%s\n' "$out" | grep -c .)));
        fi
    done
    [ "$LEGACY" = 0 ] && echo "  (no legacy debt)"
fi

echo "────────────────────────────────────────────────────────────────────────────────"
if [ "$HARD" = 0 ]; then echo "lint-drills: batch OK (9 S6-S8 drills, 0 violations)${LEGACY:+; legacy advisory findings tracked}"; exit 0
else echo "lint-drills: $HARD batch violation(s)"; exit 1; fi
