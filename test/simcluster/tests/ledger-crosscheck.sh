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
LEDGER="$REPO/docs/deploy-tier-gotchas.md"
VERDICTS="$HERE/../expected-verdicts.tsv"
FAIL=0

[ -f "$LEDGER" ]   || { echo "ledger-crosscheck: missing $LEDGER" >&2; exit 2; }
[ -f "$VERDICTS" ] || { echo "ledger-crosscheck: missing $VERDICTS" >&2; exit 2; }

# An entry is CLOSED only if its heading block says so explicitly. Everything else is OPEN — fail-closed,
# because the failure mode we are guarding against is a live defect quietly having no owner.
closed_ids() {
    awk '
        /^### (#[0-9]+|DOC-[0-9]+)/ { id=$2; sub(/[^#A-Za-z0-9-].*/,"",id); blk="" ; cur=id; next }
        cur != "" { blk = blk " " $0 }
        /^### / && cur != "" { }
        END { }
    ' "$LEDGER" 2>/dev/null
    # simpler + robust: a heading whose next 3 lines contain a closure marker
    grep -A 3 -E '^### (#[0-9]+|DOC-[0-9]+)' "$LEDGER" \
        | awk '/^### /{id=$2} /FIXED|CLOSED|已闭合|已修复|REFUTED/{if(id!="")print id}' \
        | sort -u
}

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
# `[CANDIDATE]` / `候选`. Demanding a non-GREEN owner cell for an unconfirmed finding would force the
# suite to assert something nobody has established yet, which is how a gate turns into noise and then
# gets switched off. They are owned by the ADJUDICATION batch (R6) instead, and are reported separately
# so they can never be silently forgotten either.
candidate_ids() {
    awk '/^### (#[0-9]+|DOC-[0-9]+)/{id=$2; sub(/[^#A-Za-z0-9-].*/,"",id); n=0; hdr=$0
             if (hdr ~ /CANDIDATE|候选/) { print id; id="" ; next } ; next }
         id != "" { n++; if (n<=3 && ($0 ~ /CANDIDATE|候选/)) { print id; id="" } }' "$LEDGER" | sort -u
}

OPEN=$(open_ids); OWNED=$(owned_ids); CAND=$(candidate_ids)
echo "── open registered defects vs non-GREEN owner cells ──────────────────────────"
for id in $OPEN; do
    if printf '%s\n' "$OWNED" | grep -qx -- "$id"; then
        echo "  ok        $id"
    elif printf '%s\n' "$CAND" | grep -qx -- "$id"; then
        echo "  R6-CAND   $id — [CANDIDATE] in the ledger; adjudication (CONFIRMED/REFUTED) is batch R6's exit"
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
