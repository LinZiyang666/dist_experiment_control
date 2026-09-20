# lib/ledger.sh — the ONE reader of an entry's STATUS in docs/deploy-tier-gotchas.md. POSIX sh + awk
# (sh -n + dash -n clean). Sourced by tests/ledger-crosscheck.sh (open ⇒ must be owned) and
# tests/validate-verdicts.sh (a band must not pin a closed defect): two consumers of one convention that
# used to carry two different readers and could disagree about whether a defect was closed (round-2
# review R3-F7 — one of them still read "heading + 3 body lines").
#
# THE CONVENTION. An entry's status is on its `### #N — …` / `### DOC-N — …` HEADING LINE and nowhere else
# (internal review round 1 R6-F11: a positional read carried a neighbour's word into an entry for months).
# Within the heading it is read from the TRAILING STATUS GROUP — the last `（…）` / `(…)` parenthesis, or
# the last `**…**` run when the heading ends in one (#79's shape) — clause by clause (split on `；` `;`
# `，` `,` `·`), with everything from a cross-reference (`#NN` / `DOC-NN`) to the end of its clause
# discarded. So `（已修复 by #80，2026-09-19 关闭）` is closed (the status precedes the reference), while
# `（OPEN；与 #80 的根因同族，#80 已修复）` is open (the status follows one) — round-2 review R3-F2 showed
# the plain substring read closing the latter, and an entry whose heading quoted a code token
# `NOT_FIXED`. Code spans are blanked first and the English words must stand alone.
#
#   closed     FIXED | CLOSED | REFUTED (whole words) | 已闭合 | 已修复
#   candidate  CANDIDATE (whole word) | 候选
#   open       everything else — fail-closed: the failure mode these gates guard against is a live
#              defect quietly having no owner.

# ledger_heading_status : filter — reads heading lines on stdin, prints `<id>\t<status text>` per line.
ledger_heading_status() {
    awk '
        function status_group(line,   s, n, i, t, g, k, c, out, clauses) {
            gsub(/`[^`]*`/, "", line)                         # code spans are never status
            sub(/^### (#[0-9]+|DOC-[0-9]+)/, "", line)
            s = line
            if (s ~ /[）)][[:space:]]*$/) {
                # the LAST parenthesis group: walk backwards to its opener
                sub(/[[:space:]]*$/, "", s)
                n = length(s); i = n - 1
                while (i > 0 && substr(s, i, 1) != "（" && substr(s, i, 1) != "(") i--
                g = substr(s, i + 1, n - i - 1)
            } else if (s ~ /\*\*[[:space:]]*$/) {
                sub(/[[:space:]]*$/, "", s)
                n = length(s) - 2; i = n
                while (i > 1 && substr(s, i - 1, 2) != "**") i--
                g = substr(s, i + 1, n - i)
            } else {
                g = s
            }
            gsub(/[；;，,·]/, "\n", g)
            k = split(g, clauses, "\n"); out = ""
            for (c = 1; c <= k; c++) {
                t = clauses[c]
                sub(/(#[0-9]+|DOC-[0-9]+).*$/, "", t)         # a cross-reference ends the clause here
                out = out " " t
            }
            return out
        }
        { id = $2; sub(/[^#A-Za-z0-9-].*/, "", id); printf "%s\t%s\n", id, status_group($0) }'
}

# ledger_closed_ids <ledger> : the ids whose heading status says closed, one per line, sorted.
ledger_closed_ids() {
    grep -E '^### (#[0-9]+|DOC-[0-9]+)' "$1" | ledger_heading_status \
        | awk -F'\t' '$2 ~ /(^|[^A-Za-z_])(FIXED|CLOSED|REFUTED)([^A-Za-z_]|$)|已闭合|已修复/ { print $1 }' \
        | sort -u
}

# ledger_candidate_ids <ledger> : the ids whose heading status says CANDIDATE, one per line, sorted.
ledger_candidate_ids() {
    grep -E '^### (#[0-9]+|DOC-[0-9]+)' "$1" | ledger_heading_status \
        | awk -F'\t' '$2 ~ /(^|[^A-Za-z_])CANDIDATE([^A-Za-z_]|$)|候选/ { print $1 }' \
        | sort -u
}
