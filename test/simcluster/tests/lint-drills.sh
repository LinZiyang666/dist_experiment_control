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
# SCOPE. The drills in BATCH below are CONTRACT-ENFORCED: any violation is a HARD failure (exit 1).
# Every new contract-era batch MUST append its drills to BATCH — a drill absent from this list is exempt
# from every static false-green ban above, which is the single easiest and costliest thing to forget at
# batch landing (s7-s9-plan §7, closing gate 3). Currently: the 9 S6-S8 drills + the 7 G-C (S7+S9) drills.
# The runner (run-drills.sh) additionally cross-checks every drill's verdict-line rc against its process rc, so a
# LEGACY drill from an earlier batch that still uses `…; drill_end; exit N` is caught as VERDICT-RC-MISMATCH
# at RUN time regardless of this lint. Pass --all to also print an ADVISORY report of legacy-drill debt (a
# tracked follow-up: migrate earlier batches to the verdict contract); legacy findings never fail this lint.
#
# Run:  sh tests/lint-drills.sh          (hard-gate every drill in BATCH)
#       sh tests/lint-drills.sh --all    (+ advisory legacy-debt report)
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
DRILLS_DIR="$(cd "$HERE/../drills" && pwd)"
# R1: BATCH is now EVERY drill. It used to be a hand-maintained list of 16, which left 21/37 exempt from
# every false-green ban below — and the exemption is exactly where H1 (a jq path that matched nothing, so
# one assertion could never pass and its twin was permanently vacuous) and H13 (a broken jq in drill 93)
# survived. A per-batch opt-in list cannot be the gate for a suite whose whole job is catching false greens.
BATCH="$(cd "$DRILLS_DIR" && for f in *.sh; do printf '%s ' "${f%.sh}"; done)"
ALL=0; [ "${1:-}" = --all ] && ALL=1

# PENDING <drill> <rule> — a violation that is REAL, ACKNOWLEDGED, and OWNED BY A NAMED LATER BATCH.
# It prints loudly and is NOT counted as a hard failure, but it is NOT an exemption either: the owning
# batch's exit criteria include emptying its rows here, and a PENDING row for a rule/drill pair that no
# longer violates is itself a hard failure (a stale carve-out is how allowlists rot into waivers).
#
# The ONLY current entries are `bare-not-covered`: converting those 24 `warn "…NOT-COVERED"` notes into
# real not_covered() calls IS batch R2 (docs/allgreen-remediation-roadmap.md §4). Fixing them here would
# merge R2 into R1 and skip R2's requirement that every newly-red cell be pre-enumerated with an owner.
PENDING=""   # R2 emptied it: all 24 bare-not-covered sites are now real not_covered() calls.

is_pending() { printf '%s\n' "$PENDING" | grep -q "^$1:$2:"; }
pending_owner() { printf '%s\n' "$PENDING" | grep "^$1:$2:" | cut -d: -f3; }

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
    # G-C: a HARNESS FUNCTION wrapped in `sh -c "…"`. A new shell inherits variables it was exported with
    # and nothing else — never the sourced functions. So `sh -c "! node_exists brk1"` fails with
    # "not found", `!` inverts the failure, and the assertion becomes PERMANENTLY TRUE: a false green no
    # amount of running can catch. (Caught three times while writing the G-C drills; s7-s9-plan §1.4
    # R-NOSHC. The same shape as the round-5 sigpipe rule above: a structural guard, not vigilance.)
    # assert_* runs "$@" in the CURRENT shell, so the fix is always "define a predicate function".
    printf '%s\n' "$_C" | grep -qE "sh -c [\"'][^\"']*(dexec|dexec_it|poll_until|node_running|node_exists|node_kill|node_start|node_stop|rm_node|ctr_name|net_name|vol_etc|vol_lib|tcp_refused|list_nodes|sim_leader|a_non_leader_voter|grow_to_[23]|dp_curl_ok_body|dp_curl_refused|dp_curl_blackholed|expose_serve_sentinel|vault_[a-z]+|fault_[a-z_]+|leak_[a-z_]+|ev_[a-z_]+|secrets_[a-z_]+|agent_provision_yaml)[[:space:]]" \
        && echo "noshc: a HARNESS FUNCTION inside 'sh -c' — a new shell does not inherit sourced functions, so the command dies as 'not found' (and under '!' becomes permanently TRUE = false green). Define a predicate function and pass it to assert_* instead (s7-s9-plan §1.4 R-NOSHC)"
    # G-C: `timeout N <harness-fn>` — same trap as noshc but via coreutils `timeout`, which execvp's its
    # argument directly and never sees a sourced shell function, so it dies rc=127 "No such file or
    # directory" and the arm fails for a reason unrelated to what it tests. (Stage-C caught this in drill
    # 96's D4a/D4b, where it silently turned a #57/#58 PRODUCT-RED into an ASSERT-FAIL.) Push the timeout
    # INSIDE the container in front of the real binary: `dexec … timeout N tether …`.
    printf '%s\n' "$_C" | grep -qE '\btimeout[[:space:]]+[0-9]+[[:space:]]+(dexec|dexec_it|poll_until|node_running|node_exists|node_kill|node_start|node_stop|rm_node|ctr_name|net_name|tcp_refused|list_nodes|sim_leader|dp_curl_[a-z_]+|vault_[a-z_]+|fault_[a-z_]+|leak_[a-z_]+|ev_[a-z_]+|secrets_[a-z_]+)\b' \
        && echo "timeout-fn: 'timeout N <harness-fn>' — coreutils timeout execvp's its argument and cannot see a sourced shell function (dies rc=127). Push the timeout INSIDE the container before the real binary (dexec … timeout N tether …)"
    # G-C: an UNESCAPED backtick inside a double-quoted assert description is COMMAND SUBSTITUTION —
    # the shell EXECUTES it. `assert_ok "B0a `run` is gated..."` really tries to run `run`. Prose must use
    # single quotes (or \` if a backtick is truly wanted). Caught by `dash -n` on drill 96; plain `sh -n`
    # accepts it, which is exactly why it needs a static rule rather than one shell's parser.
    printf '%s\n' "$_C" | grep -qE 'assert_(ok|refuses|setup|bug|nc)[[:space:]]+"[^"]*[^\\]`' \
        && echo "backtick-in-desc: an unescaped backtick inside a quoted assert description — the shell COMMAND-SUBSTITUTES it (it will actually run). Use single quotes in prose"
    # EXT-REVIEW-B5: a combined `trap '<fn>' EXIT INT TERM` is WRONG for INT/TERM — a POSIX signal handler
    # RETURNS to the next command after cleanup, so a Ctrl-C mid-drill runs cleanup then RESUMES executing
    # kills / iptables / cert overwrites / volume disasters (and races cmd_drill's own nuke). Drills must
    # use drill_install_traps (lib/assert.sh), which registers EXIT separately and makes INT/TERM exit
    # 128+signo without resuming.
    printf '%s\n' "$_C" | grep -qE "trap[[:space:]]+.[^']*.[[:space:]]+(EXIT[[:space:]]+INT|INT[[:space:]]+TERM|EXIT[[:space:]]+INT[[:space:]]+TERM)" \
        && echo "combined-signal-trap: a single trap on EXIT+INT/TERM resumes execution after the handler returns (POSIX) — a Ctrl-C keeps running destructive steps. Use drill_install_traps <cleanup-fn> (lib/assert.sh), which exits 128+signo on INT/TERM"
    # GATE D (INVERTED same-block pairing) — WITHDRAWN, deliberately NOT implemented (external review M-5,
    # release-readiness-followups §6.4). An INVERTED block asserts "the defect reproduced" by a POSITIVE
    # predicate (assert_ok on a command that SHOULD have failed); to still land non-GREEN it must be PAIRED
    # with a product_red/assert_bug in the SAME block. Enforcing that pairing is not decidable by the 13
    # line-global grep gates here — it needs block-scoped parsing (awk/state) — and ANY implementable proxy
    # keys on the word "INVERTED" in COMMENTS, i.e. a prose-triggered lint: rename the comment and the gate
    # goes blind; mention INVERTED in an unrelated note and it false-fires. That is the exact failure mode
    # kept-sites' comment-stripping (code() above) exists to avoid, and this repo already WROTE, MEASURED,
    # and WITHDREW such a static sibling (ledger-crosscheck.sh header: 10 hits, 3 real). Replacement, by half:
    #   REGISTERED defect sitting in a GREEN drill  -> tests/ledger-crosscheck.sh (executing, fail-closed).
    #   UNREGISTERED inverted false-green           -> NOT statically checkable; owned by the Stage-C
    #     non-vacuity requirement (roadmap §3 gate C: every changed/new assertion proven RED against a bad
    #     input). Accepted residual risk, recorded here so nobody re-invents this prose-keyed gate.
    # R1/H12: an EMPTY grep needle matches every line, so `grep -qE ''` is a permanently-true oracle. Same
    # family as H1 (a jq path that matches nothing is permanently FALSE, and its negation permanently true).
    # A static rule can catch the empty needle; it CANNOT catch a well-formed-but-wrong jq path — that is
    # what the non-vacuity proof requirement exists for (roadmap §3 gate C: every changed/new assertion must
    # be shown to go RED against a deliberately bad input).
    printf '%s\n' "$_C" | grep -qE "grep -[qEo]*[[:space:]]+(\"\"|'')" \
        && echo "empty-needle: grep with an EMPTY pattern matches everything — a permanently-true oracle"
    printf '%s\n' "$_C" | grep -qE '(^|[^_])drill_begin[[:space:]]' || echo "no-frame: missing drill_begin"
    printf '%s\n' "$_C" | grep -qE '(^|[^_])drill_end'             || echo "no-frame: missing drill_end (no verdict line)"
}

is_batch() { case " $BATCH " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

HARD=0; LEGACY=0
echo "── contract-enforced batch (S6-S8 + G-C) ──────────────────────────────────────"
for name in $BATCH; do
    f="$DRILLS_DIR/$name.sh"
    [ -f "$f" ] || { echo "  MISSING  $name.sh"; HARD=$((HARD+1)); continue; }
    out=$(scan_file "$f")
    if [ -n "$out" ]; then
        # Split the findings into PENDING (acknowledged, owned by a named later batch) and HARD.
        _hard_n=0
        printf '%s\n' "$out" | while IFS= read -r v; do
            _rule=${v%%:*}
            if is_pending "$name" "$_rule"; then echo "  PENDING   $name: $_rule (owner: $(pending_owner "$name" "$_rule")) — $v"
            else echo "  VIOLATION $name: $v"; fi
        done
        _hard_n=$(printf '%s\n' "$out" | while IFS= read -r v; do
            _rule=${v%%:*}; is_pending "$name" "$_rule" || echo x
        done | grep -c .)
        HARD=$((HARD+_hard_n))
        [ "$_hard_n" = 0 ] && echo "  ok   $name (all findings PENDING)"
    else echo "  ok   $name"; fi
done

# A PENDING row whose violation no longer exists is a STALE carve-out — the mechanism by which an
# allowlist rots into a permanent waiver. Fail hard so the owning batch has to delete its rows.
printf '%s\n' "$PENDING" | while IFS= read -r _row; do
    [ -n "$_row" ] || continue
    _pd=${_row%%:*}; _rest=${_row#*:}; _pr=${_rest%%:*}
    if [ -f "$DRILLS_DIR/$_pd.sh" ] && ! scan_file "$DRILLS_DIR/$_pd.sh" | grep -q "^$_pr:"; then
        echo "  STALE-PENDING $_pd: rule '$_pr' no longer fires — delete this PENDING row"
    fi
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
if [ "$HARD" = 0 ]; then echo "lint-drills: batch OK ($(set -- $BATCH; echo $#) contract-enforced drills, 0 violations)${LEGACY:+; legacy advisory findings tracked}"; exit 0
else echo "lint-drills: $HARD batch violation(s)"; exit 1; fi
