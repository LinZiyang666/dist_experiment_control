#!/bin/sh
# ledger-crosscheck.sh — every OPEN registered defect must be pinned by a NON-GREEN cell.
#
# WHY THIS EXISTS, AND WHY IT IS NOT A GREP RULE (R1 → R2 hand-off).
# R1 tried to enforce this statically: "a drill whose executed code names #NN must hold a
# product_red/assert_bug". That rule was WRITTEN, MEASURED, and WITHDRAWN — it fired on 10 drills of which
# only 3 were real, because a static scan cannot tell
#     "this drill PINS a live defect"            (94's `#49-hardening` asserts the fix HOLDS)
# from
#     "this drill REGRESSION-TESTS a fixed one"  (32's #28/#31, 73's #29/#30/#32/#33 are context)
# and the difference is not in the drill text at all — it is in whether the defect is still open. The
# gotcha ledger knows that; grep never will. So the check lives HERE, joining two ledgers:
#
#   docs/deploy-tier-gotchas.md   — which defects are still OPEN   (the authority on "is it live?")
#   test/simcluster/expected-verdicts.tsv — which drill cell owns each one (the authority on "who pins it?")
#
# The invariant: an OPEN defect with no non-GREEN owner cell is a defect nothing can catch. That is the
# exact shape of #25/#26/#27 sitting inside three "green" drills (80/81/82) for two batches.
#
# Run: sh tests/ledger-crosscheck.sh
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
# LEDGER / VERDICTS may be overridden from the environment so the selftest can run this gate against
# synthetic ledgers (tests/ledger-crosscheck-selftest.sh); production callers set neither.
LEDGER="${LEDGER:-$REPO/docs/deploy-tier-gotchas.md}"
VERDICTS="${VERDICTS:-$HERE/../expected-verdicts.tsv}"
FAIL=0

[ -f "$LEDGER" ]   || { echo "ledger-crosscheck: missing $LEDGER" >&2; exit 2; }
[ -f "$VERDICTS" ] || { echo "ledger-crosscheck: missing $VERDICTS" >&2; exit 2; }

# An entry's STATUS IS READ FROM ITS HEADING LINE AND NOWHERE ELSE. The ledger's convention is that the
# `### #N — …` line carries the state in its trailing parenthesis — `（已修复，…）` / `FIXED` / `CLOSED`,
# `（OPEN）`, `（CANDIDATE，未归因）` — and that is the only place this gate looks. The first version read
# "the heading plus its next 3 lines" for closure and "heading or first 3 body lines" for CANDIDATE. That
# is POSITIONAL: a cross-reference in an entry's second line ("源码 SB-96-3 已闭合行为面", "#32（CANDIDATE）")
# carries the neighbour's word into this entry's status, and DOC-28 was in fact read as closed for months
# on the strength of a sentence about a different thing (internal review round 1 R6-F11 — the #33 fix had
# moved the offending text rather than hardening the read). Everything not marked closed in its heading is
# OPEN — fail-closed, because the failure mode we guard against is a live defect quietly having no owner.
#
# HOW the heading is read (trailing status group, clause by clause, cross-references end their clause)
# lives in lib/ledger.sh — the ONE reader, shared with tests/validate-verdicts.sh so the two gates cannot
# disagree about whether a defect is closed (round-2 review R3-F2 / R3-F7).
. "$HERE/../lib/ledger.sh"
closed_ids() { ledger_closed_ids "$LEDGER"; }

open_ids() {
    all=$(grep -oE '^### (#[0-9]+|DOC-[0-9]+)' "$LEDGER" | sed 's/^### //' | sort -u)
    cls=$(closed_ids)
    for i in $all; do printf '%s\n' "$cls" | grep -qx -- "$i" || printf '%s\n' "$i"; done
}

# owners: every non-GREEN row of the verdict ledger contributes its owner field (may list several).
# The owner column moved 3 -> 5 when expected-verdicts.tsv was split into a strict machine table plus
# expected-verdicts-log.md (the prose ledger). Rows are now selected by "not a comment, exactly 6
# fields" rather than by line number, so adding or removing header comments cannot silently drop rows —
# which would have made this gate report every defect as UNOWNED, or worse, none at all.
owned_ids() {
    awk -F'\t' '!/^#/ && NF==6 && $2!="GREEN" {print $5}' "$VERDICTS" \
        | tr ' /,+' '\n\n\n\n' | grep -oE '#[0-9]+|DOC-[0-9]+' | sort -u
}

# CANDIDATE entries are, by definition, NOT yet confirmed defects — the ledger marks them
# `CANDIDATE` / `候选` IN THE HEADING (same heading-only rule as closure, same reason). Demanding a
# non-GREEN owner cell for an unconfirmed finding would force the suite to assert something nobody has
# established yet, which is how a gate turns into noise and then gets switched off. They are owned by the
# ADJUDICATION batch (R6) instead, and are reported separately so they can never be silently forgotten.
candidate_ids() { ledger_candidate_ids "$LEDGER"; }

# BY-DESIGN entries name no defect at all: they record a deliberate trade-off that constrains OPERATOR
# ACTION (e.g. "N>=2 must upgrade in lockstep"), so there is nothing for a drill to catch and demanding a
# non-GREEN owner would force the suite to assert a failure that must not happen.
#
# THE EXEMPTION IS NOT FREE. It applies only to a block that ALSO writes down its REVERSAL CONDITION —
# the future in which the trade-off stops holding and the gates must be built. Without that, "by design"
# is just a label anyone can staple onto a live defect to make this gate quiet, which is precisely the
# permanent-waiver failure every ledger in this repo is built to avoid (CLAUDE.md: "豁免必须自带过期压力").
# The reversal condition is what gives the exemption an expiry.
#
# The block ENDS at the next heading of ANY level, not just the next `### `. That distinction is not
# hypothetical: the last `### ` entry in the file is followed by `## 已了结条目索引`, a table whose rows
# literally read "WONTFIX-BY-DESIGN". A scanner that ran to EOF swallowed that table into the final
# entry's block and handed it a by-design marker it never wrote — caught by mutating the real entry's
# status line and watching the exemption survive anyway.
bydesign_ids() {
    awk '/^#+ /{ if (id != "" && byd && rev) print id; id=""; byd=0; rev=0 }
         /^### (#[0-9]+|DOC-[0-9]+)/{ id=$2; sub(/[^#A-Za-z0-9-].*/,"",id); next }
         id != "" { if ($0 ~ /BY-DESIGN|by design/) byd=1
                    if ($0 ~ /反转条件|reversal condition/) rev=1 }
         END { if (id != "" && byd && rev) print id }' "$LEDGER" | sort -u
}

OPEN=$(open_ids); OWNED=$(owned_ids); CAND=$(candidate_ids); BYD=$(bydesign_ids)
echo "── open registered defects vs non-GREEN owner cells ──────────────────────────"
for id in $OPEN; do
    if printf '%s\n' "$OWNED" | grep -qx -- "$id"; then
        echo "  ok        $id"
    elif printf '%s\n' "$CAND" | grep -qx -- "$id"; then
        echo "  R6-CAND   $id — [CANDIDATE] in the ledger; adjudication (CONFIRMED/REFUTED) is batch R6's exit"
    elif printf '%s\n' "$BYD" | grep -qx -- "$id"; then
        echo "  BY-DESIGN $id — a documented trade-off constraining operator action, WITH a written reversal condition; no drill can or should go non-GREEN for it"
    else
        echo "  UNOWNED   $id — open in the gotcha ledger but no non-GREEN cell in expected-verdicts.tsv pins it"
        FAIL=$((FAIL+1))
    fi
done

echo "──────────────────────────────────────────────────────────────────────────────"
if [ "$FAIL" = 0 ]; then
    echo "ledger-crosscheck: OK ($(printf '%s\n' "$OPEN" | grep -c .) open defect(s), all pinned by a non-GREEN cell)"
    exit 0
fi
echo "ledger-crosscheck: $FAIL open defect(s) with NO non-GREEN owner — they cannot be caught by any run" >&2
exit 1
