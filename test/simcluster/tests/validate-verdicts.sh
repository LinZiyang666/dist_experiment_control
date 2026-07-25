#!/bin/sh
# validate-verdicts.sh — strict validator for the MACHINE half of the verdict expectation table.
# POSIX sh. No docker, no server. Run: sh tests/validate-verdicts.sh
#
# WHY THIS EXISTS. `expected-verdicts.tsv` used to mix a machine expectation with a prose changelog —
# one row ran to 4,826 characters — so nothing could parse it and `run-drills.sh` never even opened it.
# The consequence was measured on 2026-07-23: the runner printed `14 BLOCKER(S)` of which NINE were
# recorded, owned, deliberate INCOMPLETEs, and the two rows that were a REAL product regression
# (c6b9c9e's mandatory --reset-js gate, unswept to the drill call sites) were indistinguishable from the
# background. Splitting the file is what makes deviation detection computable; this validator is what
# keeps the machine half machine-readable.
#
# THE LAUNDERING SURFACE IS `bands`. A band pre-authorizes a red, so every rule below exists to stop a
# band from becoming a blanket pardon:
#   - a band MUST name an open defect (#NN / DOC-NN) — a band with no defect is a wish, not a pin;
#   - a band MUST name a signature slug defined in expected-verdicts-log.md — a verdict-enum-only band
#     ("any ASSERT-FAIL in this drill is fine") would blind that drill to a NEW red, which is exactly the
#     failure this whole increment exists to prevent;
#   - a band whose defect the gotcha ledger records as CLOSED is REJECTED — bands are debt, and debt that
#     has been paid must stop being carried.
# A banded red still BLOCKS at run time. This file governs what may be declared, never what may be waived.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
SIMDIR="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$SIMDIR/../.." && pwd)"
TSV="${TETHER_VERDICTS_TSV:-$SIMDIR/expected-verdicts.tsv}"
LOG="${TETHER_VERDICTS_LOG:-$SIMDIR/expected-verdicts-log.md}"
# Overridable so the self-test (tests/validate-verdicts-selftest.sh) can point every input at a throwaway
# sandbox. Real runs use the defaults and are unaffected.
LEDGER="${TETHER_LEDGER:-$REPO/docs/deploy-tier-gotchas.md}"
DRILLDIR="${TETHER_DRILLDIR:-$SIMDIR/drills}"
FAIL=0

fail() { printf 'validate-verdicts: %s\n' "$*" >&2; FAIL=$((FAIL+1)); }

[ -f "$TSV" ] || { fail "missing $TSV"; exit 2; }
[ -f "$LOG" ] || { fail "missing $LOG"; exit 2; }
[ -d "$DRILLDIR" ] || { fail "missing $DRILLDIR"; exit 2; }

# Data rows only: comments start with '#'. Blank lines are skipped.
data_rows() { grep -v '^#' "$TSV" | grep -v '^[[:space:]]*$'; }

# ── per-row structural validation ───────────────────────────────────────────────────────────────────
data_rows | while IFS= read -r line; do
    n=$(printf '%s' "$line" | awk -F'\t' '{print NF}')
    drill=$(printf '%s' "$line" | cut -f1)
    if [ "$n" -ne 6 ]; then
        printf 'ROW-FIELDS\t%s\t%s\n' "$drill" "$n"
        continue
    fi
    expected=$(printf '%s' "$line" | cut -f2)
    ncgap=$(printf '%s' "$line" | cut -f3)
    bands=$(printf '%s' "$line" | cut -f4)
    owner=$(printf '%s' "$line" | cut -f5)
    noteref=$(printf '%s' "$line" | cut -f6)

    # External review Major 2: a band pre-authorizes a red, and a non-GREEN expectation is debt — both
    # need an OWNER. An empty/`-` owner on either was accepted before, so an unsupported band could
    # produce MATCH-BAND with nobody accountable. (The owner column was never read at all.)
    if [ "$bands" != "-" ] && { [ -z "$owner" ] || [ "$owner" = "-" ]; }; then
        printf 'BAND-NO-OWNER\t%s\n' "$drill"
    fi

    case "$expected" in
        GREEN|ASSERT-FAIL|SETUP-RED|PRODUCT-RED|INCOMPLETE) ;;
        *) printf 'BAD-ENUM\t%s\t%s\n' "$drill" "$expected" ;;
    esac

    case "$ncgap" in
        -|''|*[!0-9]*) [ "$ncgap" = "-" ] || printf 'BAD-NCGAP\t%s\t%s\n' "$drill" "$ncgap" ;;
    esac
    # GREEN carries zero coverage gaps BY THE VERDICT CONTRACT (assert.sh: any not_covered lands
    # INCOMPLETE). A GREEN row declaring a non-zero gap is self-contradictory, not merely odd.
    if [ "$expected" = GREEN ] && [ "$ncgap" != 0 ]; then
        printf 'GREEN-NCGAP\t%s\t%s\n' "$drill" "$ncgap"
    fi
    # Conversely an INCOMPLETE row pinned at 0 gaps could never be met.
    if [ "$expected" = INCOMPLETE ] && [ "$ncgap" = 0 ]; then
        printf 'INCOMPLETE-ZERO\t%s\n' "$drill"
    fi

    if [ "$bands" != "-" ]; then
        # `printf '%s\n'`, NOT `printf '%s'`: without the trailing newline `read` consumes the final
        # (here: only) field but returns non-zero, so the loop body never runs and every band check
        # below is silently VACUOUS. Caught by the mutation test "band with no signature", which passed
        # a malformed band while this validator reported OK.
        printf '%s\n' "$bands" | tr ',' '\n' | while IFS= read -r b; do
            [ -n "$b" ] || continue
            bv=${b%%@*};   rest=${b#*@}
            bid=${rest%%@*}; sig=${rest#*@}
            case "$bv" in
                ASSERT-FAIL|SETUP-RED|PRODUCT-RED|INCOMPLETE) ;;
                *) printf 'BAND-ENUM\t%s\t%s\n' "$drill" "$b" ;;
            esac
            # Exact numeric grammar, not a shell glob: `'#'[0-9]*` matched `#1abc`. Require #<digits> or
            # DOC-<digits> exactly (external review Major 2, defect-ID syntax).
            if ! printf '%s' "$bid" | grep -qE '^(#[0-9]+|DOC-[0-9]+)$'; then
                printf 'BAND-NO-DEFECT\t%s\t%s\n' "$drill" "$b"
            fi
            case "$sig" in
                sig:?*) ;;
                *) printf 'BAND-NO-SIG\t%s\t%s\n' "$drill" "$b" ;;
            esac
        done
    fi

    grep -q "^## $noteref\$" "$LOG" || printf 'NOTE-REF-MISSING\t%s\t%s\n' "$drill" "$noteref"
done > "${TMPDIR:-/tmp}/vv-rows.$$" 2>/dev/null

while IFS= read -r r; do
    [ -n "$r" ] || continue
    fail "$r"
done < "${TMPDIR:-/tmp}/vv-rows.$$"
rm -f "${TMPDIR:-/tmp}/vv-rows.$$"

# Every signature slug a band names must be DEFINED in the prose log with the EXACT grammar the runtime
# resolver uses — `sig:<slug> := <ERE>` (run-drills.sh:_sig_regex), NOT a free-form prose mention.
# External review Major 2: `grep -qF "$sig"` matched a sentence that merely NAMES the slug, so a band
# could resolve to nothing at run time while the validator passed. Require exactly one real definition,
# and that its ERE be non-empty and compile. This runs UNCONDITIONALLY (MI7).
for b in $(data_rows | cut -f4 | grep -v '^-$' | tr ',' ' '); do
    [ -n "$b" ] || continue
    sig=${b##*@}
    case "$sig" in sig:?*) ;; *) continue ;; esac
    slug=${sig#sig:}
    # External review re-review Medium 6: the slug is interpolated UNESCAPED into grep/sed here AND into
    # run-drills.sh's `sed -n "s/…sig:$slug…//p"`. A slug with a sed metacharacter (e.g. `x/y` — the `/`
    # terminates the sed s/// command) is accepted by a loose `sig:?*` check but resolves to EMPTY at
    # runtime, so the band can never match. Enforce ONE literal safe grammar shared by both:
    # `[A-Za-z0-9][A-Za-z0-9._-]*` (no `/`, no regex/sed metacharacters).
    case "$slug" in
        *[!A-Za-z0-9._-]*|'') fail "BAND-SIG-BADSLUG: slug '$slug' in '$sig' has characters outside [A-Za-z0-9._-]; runtime's sed resolver cannot safely interpolate it"; continue ;;
    esac
    case "$slug" in [!A-Za-z0-9]*) fail "BAND-SIG-BADSLUG: slug '$slug' must start with an alphanumeric"; continue ;; esac
    # Same extraction as run-drills.sh _sig_regex, but count definitions and validate the ERE.
    ndef=$(grep -cE "^[[:space:]]*sig:$slug[[:space:]]*:=[[:space:]]*." "$LOG" 2>/dev/null || echo 0)
    case "$ndef" in ''|*[!0-9]*) ndef=0 ;; esac
    if [ "$ndef" -eq 0 ]; then
        fail "BAND-SIG-UNDEFINED: '$sig' has no 'sig:$slug := <ERE>' definition in expected-verdicts-log.md (a prose mention is not a definition)"
    elif [ "$ndef" -gt 1 ]; then
        fail "BAND-SIG-AMBIGUOUS: '$sig' has $ndef definitions in expected-verdicts-log.md; exactly one is required"
    else
        ere=$(sed -n "s/^[[:space:]]*sig:$slug[[:space:]]*:=[[:space:]]*//p" "$LOG" | head -1)
        # grep on EMPTY input returns 1 (no match, pattern OK) or 2 (malformed pattern); rc 2 = bad ERE.
        printf '' | grep -E "$ere" >/dev/null 2>&1
        [ "$?" -eq 2 ] && fail "BAND-SIG-INVALID: '$sig' ERE does not compile: $ere"
    fi
done

# External review Major 2: a band must name a defect that EXISTS in the ledger (open), not merely one
# that is "not closed". #777 (never in the ledger) was accepted before. Parse the ledger's OPEN heading
# IDs and require every band's defect to be a member. (A closed defect is caught by BAND-ON-CLOSED below.)
if [ -f "$LEDGER" ]; then
    all_ids=$(grep -oE '^### (#[0-9]+|DOC-[0-9]+)' "$LEDGER" | sed 's/^### //' | sort -u)
    for b in $(data_rows | cut -f4 | grep -v '^-$' | tr ',' ' '); do
        [ -n "$b" ] || continue
        rest=${b#*@}; bid=${rest%%@*}
        case "$bid" in '#'*|DOC-*) ;; *) continue ;; esac
        printf '%s\n' "$all_ids" | grep -qx -- "$bid" || \
            fail "BAND-UNKNOWN-DEFECT: band '$b' names $bid, which has no '### $bid' heading in the gotcha ledger"
    done
fi

# External review Major 2: duplicate drill rows make table authority depend silently on row order
# (_exp_field uses the FIRST match). Reject them before any consumer runs.
dups=$(data_rows | cut -f1 | sort | uniq -d)
if [ -n "$dups" ]; then
    for d in $dups; do fail "DUPLICATE-DRILL: '$d' appears in more than one row of expected-verdicts.tsv"; done
fi

# ── bands must not pin a CLOSED defect ──────────────────────────────────────────────────────────────
# MI7: the ledger is MANDATORY. Without it the closed-defect check cannot run, and a validator that
# cannot run a rule must fail closed, not pass silently.
[ -f "$LEDGER" ] || { fail "missing gotcha ledger $LEDGER — cannot verify bands do not pin a closed defect"; }
if [ -f "$LEDGER" ]; then
    # Same closure discipline as tests/ledger-crosscheck.sh: a heading is CLOSED only if it says so.
    closed=$(grep -A 3 -E '^### (#[0-9]+|DOC-[0-9]+)' "$LEDGER" \
        | awk '/^### /{id=$2} /FIXED|CLOSED|已闭合|已修复|REFUTED/{if(id!="")print id}' | sort -u)
    for b in $(data_rows | cut -f4 | grep -v '^-$' | tr ',' ' '); do
        [ -n "$b" ] || continue
        rest=${b#*@}; bid=${rest%%@*}
        if printf '%s\n' "$closed" | grep -qx -- "$bid"; then
            fail "BAND-ON-CLOSED-DEFECT: band '$b' pins $bid, which the gotcha ledger records as closed — delete the band"
        fi
    done
fi

# ── the table and the drills on disk must agree, both ways ──────────────────────────────────────────
# A drill with no row cannot be judged against an expectation; a row with no drill is dead weight that
# quietly makes the deviation report incomplete.
for f in "$DRILLDIR"/[0-9]*.sh; do
    [ -e "$f" ] || continue
    d=$(basename "$f" .sh)
    data_rows | cut -f1 | grep -qx -- "$d" || fail "DRILL-UNLISTED: drills/$d.sh has no row in expected-verdicts.tsv"
done
data_rows | cut -f1 | while IFS= read -r d; do
    [ -f "$DRILLDIR/$d.sh" ] || printf 'ROW-ORPHAN\t%s\n' "$d"
done > "${TMPDIR:-/tmp}/vv-orph.$$"
while IFS= read -r r; do
    [ -n "$r" ] || continue
    fail "$r — row present but drills/$(printf '%s' "$r" | cut -f2).sh does not exist"
done < "${TMPDIR:-/tmp}/vv-orph.$$"
rm -f "${TMPDIR:-/tmp}/vv-orph.$$"

n=$(data_rows | wc -l | tr -d ' ')
if [ "$FAIL" = 0 ]; then
    echo "validate-verdicts: OK ($n rows, strict 6-column form, all note-refs resolve, no band defects)"
    exit 0
fi
echo "validate-verdicts: $FAIL problem(s)" >&2
exit 1
