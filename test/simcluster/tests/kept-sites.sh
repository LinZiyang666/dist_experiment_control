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
# DRILLS defaults to the real suite; overridable via env ONLY so the N-7 self-test (kept-sites-selftest.sh)
# can point it at a scratch fixture. Production callers never set it.
DRILLS="${DRILLS:-$SIM_ROOT/drills}"
. "$SIM_ROOT/lib/manifest.sh"

# count_sites <file> [<owner-map>] : emit the kept-site count for one drill — the total on the first line
# and, when an owner map (`<line>\t<owner>`, lib/manifest.sh manifest_line_arms) is given, one
# `<owner>\t<count>` line per owner that has at least one site.
#
# The tokenizer normalises each non-comment line by turning every shell command separator into a newline,
# then inspects the FIRST WORD of each resulting fragment. A first word that is one of the six primitives is
# a call site; anything else (including `#`, so trailing comments die here too) is prose or another command.
count_sites() {
    _cs_map="${2:-/dev/null}"
    awk -v mapfile="$_cs_map" '
        BEGIN {
            split("assert_ok assert_refuses assert_setup assert_bug product_red not_covered", a, " ")
            for (i in a) K[a[i]] = 1
            n = 0
        }
        # The owner map is read first. Matched by NAME, not by FNR==NR: an empty map (/dev/null) yields no
        # records, so FNR==NR would stay true on the drill and swallow every line of it as map rows.
        FILENAME == mapfile { owner[$1] = $2; next }
        /^[ \t]*#/ { next }                       # whole-line comment: never a call site
        {
            line = $0
            # QUOTE MASK (N-7): erase quoted-string CONTENTS before separator-splitting, so ; & | ( ) { }
            # inside a description/prose string can never open a phantom command position. \047 is octal for
            # a single quote (a bare single-quote char is impossible inside this single-quoted awk program,
            # hence the octal escape). The double-quote arm handles a backslash-escaped quote inside a span.
            # Verified: identical per-drill counts on all 37 drills vs the quote-unaware tokenizer (any
            # future divergence = a real miscount to investigate).
            gsub(/\047[^\047]*\047|"(\\.|[^"\\])*"/, "Q", line)
            gsub(/[;&|(){}]/, "\n", line)         # every separator opens a new command position
            m = split(line, part, "\n")
            for (i = 1; i <= m; i++) {
                w = part[i]
                sub(/^[ \t]*/, "", w)
                # `then` / `else` / `do` / `if` / `elif` / `while` / `until` / `!` may legally precede a
                # command on the same fragment and still RUN it (`if ! assert_ok …; then`). The list is the
                # same as in assert-identity.sh — the two tokenizers must agree on what a site is, and
                # assert-identity is the one that turns the remaining wrapper shapes into a loud BLIND
                # failure (internal review round 1 R3-8), so this file need not repeat that check.
                while (w ~ /^(then|else|do|if|elif|while|until|!)[ \t]+/) sub(/^[a-z!]+[ \t]+/, "", w)
                if (match(w, /^[A-Za-z_][A-Za-z0-9_]*/)) {
                    kw = substr(w, 1, RLENGTH)
                    if (kw in K) { n++; if (FNR in owner) per[owner[FNR]]++ }
                }
            }
        }
        END {
            print n + 0
            for (o in per) printf "%s\t%d\n", o, per[o]
        }
    ' "$_cs_map" "$1"
}

# report : print `drill<TAB>kept_sites`, one line per drill, in stable (sorted) drill order — then one
# `lib/<name>` row per drills/lib/*.sh that hosts claim primitives. origin: simcluster-speed internal review
# round 1 R3-1: setup-forcesingle.sh carries claims that five drills execute (setup_forcesingle_n2), and a
# gate that scans only drills/*.sh could not see one of them swapped or deleted. The lib rows are keyed
# `lib/…` so they can never collide with a drill name (expected-verdicts / the runner never see them).
report() {
    _rp_map="${TMPDIR:-/tmp}/kept-sites.map.$$"
    for f in "$DRILLS"/*.sh; do
        d=$(basename "$f" .sh)
        if ! manifest_has "$f"; then
            printf '%s\t%s\n' "$d" "$(count_sites "$f")"
            continue
        fi
        # ARM ROWS (plan X7 / §5.5; round-2 review R1-F2 / R3-F3): an arm-split drill gets its total AND
        # one `<drill>.<arm>` row per manifest arm plus `<drill>._shared` for the sites outside the case
        # (the fixture, the `*)` branch, drill_end). A row for an arm with no sites is printed as 0 so the
        # baseline has a key for it: a claim moved between arms lowers one row and raises another while the
        # total stays put, and only the lowered row makes --check red.
        manifest_line_arms "$f" > "$_rp_map"
        _rp_out=$(count_sites "$f" "$_rp_map")
        printf '%s\t%s\n' "$d" "$(printf '%s\n' "$_rp_out" | head -1)"
        for a in $(manifest_arms "$f") _shared; do
            printf '%s.%s\t%s\n' "$d" "$a" "$(printf '%s\n' "$_rp_out" | awk -F'\t' -v a="$a" 'NR > 1 && $1 == a { print $2; f = 1 } END { if (!f) print 0 }')"
        done
    done
    rm -f "$_rp_map"
    for f in "$DRILLS"/lib/*.sh; do
        [ -e "$f" ] || continue
        n=$(count_sites "$f")
        [ "$n" -gt 0 ] || continue
        printf 'lib/%s\t%s\n' "$(basename "$f" .sh)" "$n"
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
