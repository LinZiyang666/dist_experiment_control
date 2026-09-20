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
        # simcluster-speed X18 (rule ⑦): the ERE is matched against `[err ]` lines whose text the arm
        # split now prefixes/suffixes (unit name, arm tag), so a `$`-anchored ERE silently stops
        # matching the day the line grows a suffix; and `@` is the band separator — an ERE containing it
        # cannot round-trip through `<verdict>@<defect>@sig:<slug>` parsing in either direction.
        case "$ere" in
            *'$') fail "BAND-SIG-ANCHORED: '$sig' ERE ends with '\$' — an end anchor stops matching the moment the arm split adds a suffix to the line: $ere" ;;
        esac
        case "$ere" in
            *@*) fail "BAND-SIG-AT: '$sig' ERE contains '@', the band field separator: $ere" ;;
        esac
    fi
done
# simcluster-speed rule ⑧ (global half): one signature slug belongs to ONE row. A slug two rows share is
# a band that pre-authorizes the same red in two places — after an arm split that is how a parent's band
# quietly survives on a child it was never re-standardized for (X18: a child's band gets a NEW slug,
# calibrated from that unit's own log).
for slug in $(data_rows | awk -F'\t' '$4!="-"{n=split($4,b,","); for(i=1;i<=n;i++) if (match(b[i],/@sig:[A-Za-z0-9._-]+$/)) print $1"\t"substr(b[i],RSTART+5)}' | sort -u | cut -f2 | sort | uniq -d); do
    fail "BAND-SLUG-SHARED: 'sig:$slug' is named by more than one row; a band's signature is calibrated per unit and cannot be shared"
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
    # THE SAME closure reader as tests/ledger-crosscheck.sh — lib/ledger.sh, heading-only. The first
    # version here still read "heading + 3 body lines" while claiming the same discipline, so the two
    # gates could disagree about whether a band's defect was closed (round-2 review R3-F7).
    . "$SIMDIR/lib/ledger.sh" || fail "cannot source $SIMDIR/lib/ledger.sh"
    closed=$(ledger_closed_ids "$LEDGER")
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
    case "$d" in *.*) continue ;; esac   # child rows (<drill>.<arm>) are resolved against the manifest below
    [ -f "$DRILLDIR/$d.sh" ] || printf 'ROW-ORPHAN\t%s\n' "$d"
done > "${TMPDIR:-/tmp}/vv-orph.$$"
while IFS= read -r r; do
    [ -n "$r" ] || continue
    fail "$r — row present but drills/$(printf '%s' "$r" | cut -f2).sh does not exist"
done < "${TMPDIR:-/tmp}/vv-orph.$$"
rm -f "${TMPDIR:-/tmp}/vv-orph.$$"

# ── arm-split drills: CHILD rows keyed <drill>.<arm> (simcluster-speed plan §5.5 X4/X5/X18) ─────────
# An arm-split drill (`# arms:` manifest, lib/manifest.sh) runs as one UNIT per arm, and the runner judges
# each unit against ITS OWN row. The parent row stays (every drill on disk has a row) but becomes DERIVED:
# its expectation is the precedence join of its children, its nc_gap their sum, its bands `-`. The child
# rows are written BEFORE the first split sweep, from the parent's claim ownership — never from that
# sweep's results (X5: registering the first split run's verdicts as expectations would launder every
# arm at once). Rules, each with an injection in tests/validate-verdicts-selftest.sh:
#   ①  a child row's arm is one of the drill's manifest arms         CHILD-ORPHAN / CHILD-NO-MANIFEST / CHILD-ARM-UNKNOWN
#   ②  a manifested drill has EXACTLY one child row per arm           ARM-CHILD-COUNT
#   ③  the parent's bands are `-` (bands live on the unit they pin)   PARENT-BANDS
#   ④  parent expected == precedence join of the children             PARENT-JOIN
#   ⑤  parent nc_gap == Σ children; a child `-` (non-deterministic
#      branch) needs its own `## <drill>.<arm>` section and makes
#      the parent `-` too                                             CHILD-NCGAP-DASH-NO-SECTION / PARENT-NCGAP
#   ⑥  a child's owner is `-` or a subset of the parent's             CHILD-OWNER
#   ⑦  (above, per signature) no `$`-anchored / `@`-bearing ERE       BAND-SIG-ANCHORED / BAND-SIG-AT
#   ⑧  a child's band slug carries its arm suffix; slugs are global   CHILD-BAND-SLUG / BAND-SLUG-SHARED
. "$SIMDIR/lib/manifest.sh" || { fail "cannot source $SIMDIR/lib/manifest.sh"; }
data_rows | cut -f1 | grep '\.' | while IFS= read -r u; do
    d=${u%%.*}; a=${u#*.}
    f="$DRILLDIR/$d.sh"
    if [ ! -f "$f" ]; then printf 'CHILD-ORPHAN\t%s\n' "$u"; continue; fi
    if ! manifest_has "$f"; then printf 'CHILD-NO-MANIFEST\t%s\n' "$u"; continue; fi
    printf '%s\n' $(manifest_arms "$f") | grep -qx -- "$a" || printf 'CHILD-ARM-UNKNOWN\t%s\t%s\n' "$u" "$(manifest_arms "$f")"
done > "${TMPDIR:-/tmp}/vv-child.$$"
while IFS= read -r r; do
    [ -n "$r" ] || continue
    fail "$r — child row's drill has no manifest / arm not declared in its '# arms:'"
done < "${TMPDIR:-/tmp}/vv-child.$$"
rm -f "${TMPDIR:-/tmp}/vv-child.$$"
# ids in an owner cell: the defect ids it pins (the rest is prose for the reader).
_owner_ids() { printf '%s' "$1" | grep -oE '#[0-9]+|DOC-[0-9]+' | sort -u; }
_rank() { case "$1" in ASSERT-FAIL) echo 4 ;; SETUP-RED) echo 3 ;; PRODUCT-RED) echo 2 ;; INCOMPLETE) echo 1 ;; GREEN) echo 0 ;; *) echo 0 ;; esac; }
_name_of_rank() { case "$1" in 4) echo ASSERT-FAIL ;; 3) echo SETUP-RED ;; 2) echo PRODUCT-RED ;; 1) echo INCOMPLETE ;; *) echo GREEN ;; esac; }
for f in "$DRILLDIR"/[0-9]*.sh; do
    [ -e "$f" ] || continue
    manifest_has "$f" || continue
    d=$(basename "$f" .sh)
    parent=$(data_rows | awk -F'\t' -v d="$d" 'NF==6 && $1==d {print; exit}')
    [ -n "$parent" ] || continue    # DRILL-UNLISTED already reported it
    pexp=$(printf '%s' "$parent" | cut -f2); pnc=$(printf '%s' "$parent" | cut -f3)
    pbands=$(printf '%s' "$parent" | cut -f4); powner=$(printf '%s' "$parent" | cut -f5)
    [ "$pbands" = "-" ] || fail "PARENT-BANDS: $d has arms, so its bands ('$pbands') must move to the child row of the arm they pin and the parent must read '-'"
    sum=0; dash=0; jr=0
    for a in $(manifest_arms "$f"); do
        u="$d.$a"
        nrow=$(data_rows | awk -F'\t' -v u="$u" 'NF==6 && $1==u' | wc -l | tr -d ' ')
        if [ "$nrow" != 1 ]; then fail "ARM-CHILD-COUNT: $u has $nrow row(s) in expected-verdicts.tsv; a manifested drill needs exactly one per arm (X5 — written from the parent's claim ownership BEFORE the first split sweep)"; continue; fi
        child=$(data_rows | awk -F'\t' -v u="$u" 'NF==6 && $1==u {print; exit}')
        cexp=$(printf '%s' "$child" | cut -f2); cnc=$(printf '%s' "$child" | cut -f3)
        cbands=$(printf '%s' "$child" | cut -f4); cowner=$(printf '%s' "$child" | cut -f5)
        cr=$(_rank "$cexp"); [ "$cr" -gt "$jr" ] && jr=$cr
        if [ "$cnc" = "-" ]; then
            dash=1
            grep -q "^## $u\$" "$LOG" || fail "CHILD-NCGAP-DASH-NO-SECTION: $u declares nc_gap '-' (a non-deterministic branch) but expected-verdicts-log.md has no '## $u' section saying which branch and why"
        else
            case "$cnc" in ''|*[!0-9]*) ;; *) sum=$((sum + cnc)) ;; esac
        fi
        if [ "$cowner" != "-" ]; then
            cids=$(_owner_ids "$cowner"); pids=$(_owner_ids "$powner")
            if [ -n "$cids" ]; then
                for id in $cids; do printf '%s\n' "$pids" | grep -qx -- "$id" || fail "CHILD-OWNER: $u owner names $id, which the parent $d owner cell ('$powner') does not — a child may only pin a subset of its parent's debt (X4)"; done
            else
                case "$powner" in *"$cowner"*) ;; *) fail "CHILD-OWNER: $u owner '$cowner' is neither '-' nor part of the parent's owner cell ('$powner') (X4)" ;; esac
            fi
        fi
        if [ "$cbands" != "-" ]; then
            for b in $(printf '%s' "$cbands" | tr ',' ' '); do
                slug=$(printf '%s' "$b" | sed -n 's/.*@sig:\([A-Za-z0-9._-]*\)$/\1/p')
                case "$slug" in *".$a"|*"-$a") ;; *) fail "CHILD-BAND-SLUG: $u band '$b' — a child's signature slug must end in its arm suffix ('.$a' or '-$a'): it is calibrated from THIS unit's log, not inherited from the parent (X18)" ;; esac
            done
        fi
    done
    jv=$(_name_of_rank "$jr")
    [ "$pexp" = "$jv" ] || fail "PARENT-JOIN: $d expected '$pexp' but the precedence join of its children is '$jv' — the parent row is derived, change the children (or the claim) not the parent"
    if [ "$dash" = 1 ]; then
        [ "$pnc" = "-" ] || fail "PARENT-NCGAP: $d nc_gap '$pnc' but a child declares '-', so the parent must be '-' too"
    else
        [ "$pnc" = "$sum" ] || fail "PARENT-NCGAP: $d nc_gap '$pnc' != Σ children ($sum) — a drill-level gap is recorded once per arm (X8) and the parent carries the sum"
    fi
done

n=$(data_rows | wc -l | tr -d ' ')
if [ "$FAIL" = 0 ]; then
    echo "validate-verdicts: OK ($n rows, strict 6-column form, all note-refs resolve, no band defects)"
    exit 0
fi
echo "validate-verdicts: $FAIL problem(s)" >&2
exit 1
