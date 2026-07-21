#!/bin/sh
# kept-sites.sh — the ANTI-DEBT-BY-DELETION gate for the drill suite. POSIX sh (sh -n + dash -n clean).
#
#     cd test/simcluster && sh tests/kept-sites.sh                    # print  drill<TAB>kept_sites
#     cd test/simcluster && sh tests/kept-sites.sh --check base.tsv   # fail if any drill LOST sites
#
# WHAT IT COUNTS. `kept_sites` is the number of EXECUTABLE call sites, per drill, of the six assertion
# primitives that constitute a claim about the product:
#
#     assert_ok  assert_refuses  assert_setup  assert_bug  product_red  not_covered
#
# It is a count of QUESTIONS ASKED, not of answers received. That is the whole point:
#
#   • NEUTRAL to re-classification. Turning an inverted `assert_ok "X is broken" true` into an honest
#     `product_red "X is broken (#NN)"` — the single most common repair in this suite — moves a site from
#     one primitive to another. The total is unchanged, so the gate never punishes telling the truth.
#   • FATAL to deletion. Dropping an arm, commenting an assertion out, or collapsing three checks into one
#     to "get to green" lowers the count and REDS the gate. Coverage can only be traded, never quietly
#     surrendered.
#
# WHY NOT USE `pass`? Because `pass` is not a count of assertions. In lib/assert.sh, `_as_pass` is called
# ONLY from assert_ok / assert_refuses / assert_setup — assert_bug, product_red and not_covered never touch
# it. So a drill that converts an inverted assert_ok into a product_red loses a `pass` while gaining a
# PRODUCT-RED: a pass-based gate would fire on exactly the repair it is supposed to encourage, and would be
# silent on a drill that deletes a not_covered. `pass` measures outcome; this gate must measure intent.
#
# WHY COMMENT-STRIPPING IS MANDATORY. These drills document their own findings inline, so prose like
# "promote R3 to assert_refuses" and "recording not_covered instead of a green" is everywhere. Counting raw
# keyword hits would let a drill inflate its score by writing ABOUT assertions, and would make the baseline
# drift on doc-only edits. Two defences: whole-line comments are dropped outright, and on surviving lines a
# keyword only counts when it sits at a COMMAND POSITION (start of line, or right after ; & | ( ) { }), so a
# keyword named inside a description string or a trailing comment is ignored.
#
# BASELINE POLICY: higher than baseline is always allowed (that is debt being repaid). Lower is a blocker.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
SIM_ROOT="$(cd "$HERE/.." && pwd)"
DRILLS="$SIM_ROOT/drills"

# count_sites <file> : emit the kept-site count for one drill.
#
# The tokenizer normalises each non-comment line by turning every shell command separator into a newline,
# then inspects the FIRST WORD of each resulting fragment. A first word that is one of the six primitives is
# a call site; anything else (including `#`, so trailing comments die here too) is prose or another command.
count_sites() {
    awk '
        BEGIN {
            split("assert_ok assert_refuses assert_setup assert_bug product_red not_covered", a, " ")
            for (i in a) K[a[i]] = 1
            n = 0
        }
        /^[ \t]*#/ { next }                       # whole-line comment: never a call site
        {
            line = $0
            gsub(/[;&|(){}]/, "\n", line)         # every separator opens a new command position
            m = split(line, part, "\n")
            for (i = 1; i <= m; i++) {
                w = part[i]
                sub(/^[ \t]*/, "", w)
                # `then` / `else` / `do` / `!` may legally precede a command on the same fragment.
                while (w ~ /^(then|else|do|!)[ \t]+/) sub(/^[a-z!]+[ \t]+/, "", w)
                if (match(w, /^[A-Za-z_][A-Za-z0-9_]*/)) {
                    kw = substr(w, 1, RLENGTH)
                    if (kw in K) n++
                }
            }
        }
        END { print n + 0 }
    ' "$1"
}

# report : print `drill<TAB>kept_sites`, one line per drill, in stable (sorted) drill order.
report() {
    for f in "$DRILLS"/*.sh; do
        d=$(basename "$f" .sh)
        printf '%s\t%s\n' "$d" "$(count_sites "$f")"
    done
}

case "${1:-}" in
    "")
        report
        ;;
    --check)
        BASE="${2:-}"
        [ -n "$BASE" ] || { printf 'kept-sites: --check needs a baseline .tsv path\n' >&2; exit 2; }
        [ -f "$BASE" ] || { printf 'kept-sites: baseline not found: %s\n' "$BASE" >&2; exit 2; }
        CUR=$(report)
        REGRESSIONS=0
        # Drive the loop from the BASELINE: a drill that vanished from the tree entirely is the most extreme
        # form of the regression this gate exists to catch, so a missing drill counts as 0, not as "skip".
        while IFS="$(printf '\t')" read -r d want; do
            [ -n "$d" ] || continue
            case "$d" in \#*) continue ;; esac              # allow comments in a baseline file
            got=$(printf '%s\n' "$CUR" | awk -F'\t' -v d="$d" '$1 == d { print $2; found = 1 } END { if (!found) print "0" }')
            if [ "$got" -lt "$want" ] 2>/dev/null; then
                printf 'REGRESSION  %-28s kept_sites %s -> %s (lost %s)\n' "$d" "$want" "$got" "$((want - got))" >&2
                REGRESSIONS=$((REGRESSIONS + 1))
            fi
        done <<EOF
$(cat "$BASE")
EOF
        if [ "$REGRESSIONS" != 0 ]; then
            printf 'kept-sites: %s drill(s) LOST assertion sites vs %s — coverage may be re-classified, never deleted\n' \
                "$REGRESSIONS" "$BASE" >&2
            exit 1
        fi
        printf 'kept-sites: OK — no drill fell below %s\n' "$BASE"
        ;;
    *)
        printf 'usage: kept-sites.sh [--check <baseline.tsv>]\n' >&2
        exit 2
        ;;
esac
