#!/bin/sh
# arm-manifest-lint.sh — the BLOCK-LEVEL lint for arm-split drills. POSIX sh + awk (sh -n + dash -n clean).
#
#     cd test/simcluster && sh tests/arm-manifest-lint.sh            # every drills/*.sh
#     DRILLS=<dir> sh tests/arm-manifest-lint.sh                     # a scratch tree (the self-test)
#
# lint-drills.sh is 13 LINE-GLOBAL greps and says so in its own header (block-level rules were tried
# there and withdrawn). An arm-split drill is a block structure — one top-level `case "${ARM:?}" in`
# whose branches are the arms — and every way it can be wrong is a relationship between lines, so the
# rules live here, over lib/manifest.sh's parser (ONE parser for the drill, the runner and this lint).
#
# RULES (each has its own injection in tests/arm-manifest-selftest.sh):
#   R1  manifest keys spelled exactly (`# fixtures:` is a typo the runner would silently ignore) and
#       declared once each (a second line is silently ignored); arm names [A-Za-z0-9]+ and unique; at
#       least one arm
#   R2  every arm has fixture ∈ the closed vocabulary, worst ≥ 60 (integer), grows an integer between
#       the shared fixture's first grow site (floor — the lane would release the arm mid-grow) and the
#       grow capacity of the whole file (ceiling), and a forgoes token (`-` or text)
#   R3  exactly one top-level `case "${ARM:?}" in`
#   R4  the case labels are exactly the manifest's arms (both directions), plus a `*)` branch that
#       calls setup_fail (an unknown --arm must be a SETUP-RED, never a silent no-op GREEN)
#   R5  no function definition inside the case block (r9d-nonvacuity extracts oracles by function
#       and would silently see nothing); helpers go above the case
#   R6  no bare `exit` inside the case block (only assert_setup/setup_fail/die may end a drill early,
#       and they emit a verdict line; a bare exit is an INFRA-ABORT with no verdict)
#   R7  if the drill defines _gap_drill_level() (a DRILL-LEVEL structural gap, plan X8), every branch
#       calls it — a gap that belongs to the drill must be counted by every arm, or the arms that
#       skip it land a lucky GREEN on a claim the drill as a whole does not make
#   R8  `drill_end` is reachable after the top-level esac
#   R9  a drill WITHOUT `# arms:` uses no manifest keys and no ${ARM (a half-converted drill)
# BASELINE POLICY: none — this lint has no ledger. A drill either has a well-formed manifest or none.
# origin: simcluster-speed plan §5.5 (X2, X8, X25).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SIM_ROOT="$(cd "$HERE/.." && pwd)"
DRILLS="${DRILLS:-$SIM_ROOT/drills}"
. "$SIM_ROOT/lib/manifest.sh"
FAIL=0
bad() { printf 'arm-manifest-lint: %s: %s\n' "$1" "$2" >&2; FAIL=$((FAIL + 1)); }

# case_block <file> : prints the top-level ARM case as lines `<depth-1 label or ->\t<line>` between the
# case and its esac (exclusive), one physical line each, with `L\t<label>` rows at the branch labels.
# Empty output when there is no such case. Nesting of inner case/esac is tracked so their labels are
# not mistaken for arms.
case_block() {
    awk '
        # nested case/esac may sit on one line (`case "$x" in y) … ;; *) … ;; esac`), so depth is
        # counted per OCCURRENCE, not per line; a branch may also be a one-liner (`*) setup_fail … ;;`),
        # so a label is "a label at the START of a depth-1 line" and whatever follows it on that line
        # is body. Comments and quoted text are not parsed (a drill that writes the word esac in prose
        # inside the block is not a shape this lint expects to see). Regexes are STRINGS, not /…/
        # constants: gawk evaluates a regex constant passed as a parameter to a boolean.
        function count(s, re,   n) { n = 0; while (match(s, re)) { n++; s = substr(s, RSTART + RLENGTH) } return n }
        BEGIN { RE_OPEN = "(^|[;&|(){} \t])case[ \t]"; RE_CLOSE = "(^|[;&|(){} \t])esac([ \t;]|$)" }
        /^[ \t]*case "\$\{ARM:\?\}" in[ \t]*$/ && !inblk { inblk = 1; depth = 1; next }
        inblk {
            line = $0
            if (depth == 1 && match(line, /^[ \t]*[A-Za-z0-9*|]+\)/)) {
                lab = substr(line, 1, RLENGTH); sub(/^[ \t]*/, "", lab); sub(/\)$/, "", lab)
                printf "L\t%s\n", lab
                line = substr(line, RLENGTH + 1)
                if (line ~ /^[ \t]*$/) next
            }
            # a whole-line comment (column 0 included) is not code: "the case" in prose must not move depth
            code = line; if (code ~ /^[ \t]*#/) code = ""; else sub(/[ \t]#.*$/, "", code)
            depth += count(code, RE_OPEN) - count(code, RE_CLOSE)
            if (depth <= 0) { inblk = 0; next }
            printf "B\t%s\n", line
        }' "$1"
}

for f in "$DRILLS"/*.sh; do
    [ -e "$f" ] || continue
    d=$(basename "$f" .sh)
    if ! manifest_has "$f"; then
        # R9
        if grep -qE '^# (fixture|grows|worst|forgoes):' "$f"; then bad "$d" "R9 manifest keys without a '# arms:' line"; fi
        if grep -q '\${ARM' "$f"; then bad "$d" "R9 uses \${ARM} but declares no '# arms:'"; fi
        continue
    fi
    # R1
    manifest_keys_ok "$f" || bad "$d" "R1 misspelled manifest key (allowed: arms fixture grows worst forgoes)"
    # A key declared twice is read by its FIRST line and the second is silently ignored (lib/manifest.sh
    # `head -1`) — the runner and the drill would disagree with whoever edited the second one (round-2 R3-F8).
    for k in arms fixture grows worst forgoes; do
        nk=$(grep -c "^# $k:" "$f")
        [ "$nk" -le 1 ] || bad "$d" "R1 '# $k:' is declared $nk times (only the first line is read; delete the others)"
    done
    arms=$(manifest_arms "$f")
    [ -n "$arms" ] || { bad "$d" "R1 '# arms:' names no arm"; continue; }
    for a in $arms; do
        case "$a" in *[!A-Za-z0-9]*) bad "$d" "R1 arm name '$a' is not [A-Za-z0-9]+" ;; esac
    done
    dups=$(printf '%s\n' $arms | sort | uniq -d)
    [ -z "$dups" ] || bad "$d" "R1 duplicate arm name(s): $(printf '%s' "$dups" | tr '\n' ' ')"
    # R2 — the bound is grow CAPACITY (grow_to_3 = two grows), not the token count: 96's arms each run
    # `grow_to_3 2 1` once and legitimately declare `# grows: 2`; a token count of 1 would refuse them.
    tokens=$(manifest_grow_capacity "$f")
    floor=$(manifest_shared_grow_floor "$f")
    for a in $arms; do
        fx=$(manifest_value "$f" fixture "$a")
        [ -n "$fx" ] || bad "$d" "R2 arm $a has no fixture"
        [ -z "$fx" ] || manifest_fixture_ok "$fx" || bad "$d" "R2 arm $a fixture '$fx' is not in the closed vocabulary"
        w=$(manifest_value "$f" worst "$a")
        case "$w" in
            '') bad "$d" "R2 arm $a has no worst" ;;
            *[!0-9]*) bad "$d" "R2 arm $a worst '$w' is not an integer" ;;
            *) [ "$w" -ge 60 ] || bad "$d" "R2 arm $a worst $w < 60" ;;
        esac
        g=$(manifest_value "$f" grows "$a")
        case "$g" in
            '') bad "$d" "R2 arm $a has no grows" ;;
            *[!0-9]*) bad "$d" "R2 arm $a grows '$g' is not an integer" ;;
            *) [ "$g" -le "$tokens" ] || bad "$d" "R2 arm $a declares grows=$g but the file's grow call sites can perform only $tokens grow(s) (grow_to_3 counts two)"
               # The floor (round-2 R3-F8): the fixture above the case grows on every arm's behalf, so an arm
               # declaring fewer grows than that first shared grow site performs would leave the lane mid-grow.
               [ "$g" -ge "$floor" ] || bad "$d" "R2 arm $a declares grows=$g but the shared fixture above the ARM case performs $floor grow(s) before any arm runs (the lane would release it mid-grow)" ;;
        esac
        fg=$(manifest_value "$f" forgoes "$a")
        [ -n "$fg" ] || bad "$d" "R2 arm $a has no forgoes token ('-' means none)"
    done
    # R3
    ncase=$(grep -c '^[[:space:]]*case "\${ARM:?}" in[[:space:]]*$' "$f")
    [ "$ncase" -eq 1 ] || { bad "$d" "R3 expected exactly one top-level 'case \"\${ARM:?}\" in', found $ncase"; continue; }
    blk=$(case_block "$f")
    labels=$(printf '%s\n' "$blk" | awk -F'\t' '$1=="L"{print $2}')
    # R4
    for a in $arms; do
        printf '%s\n' "$labels" | grep -qx -- "$a" || bad "$d" "R4 manifest arm $a has no case label"
    done
    # while-read over a here-document, not `for l in $labels`: the `*` label would otherwise glob-expand
    # to a directory listing, and a pipeline would put bad() in a subshell where FAIL is lost.
    while IFS= read -r l; do
        case "$l" in
            ''|'*') ;;
            *) printf '%s\n' $arms | grep -qx -- "$l" || bad "$d" "R4 case label '$l' is not a manifest arm" ;;
        esac
    done <<EOF
$labels
EOF
    printf '%s\n' "$labels" | grep -qx -- '\*' || bad "$d" "R4 no '*)' branch (an unknown --arm must be a SETUP-RED)"
    star=$(printf '%s\n' "$blk" | awk -F'\t' '$1=="L"{cur=$2; next} cur=="*" && $1=="B"{print $2}')
    if printf '%s\n' "$labels" | grep -qx -- '\*'; then
        # setup_fail at a COMMAND position with quoted text blanked (the same blanking R6 uses below): a
        # `*)` branch whose only mention of setup_fail is inside a log string was passing (round-2 R3-F8).
        printf '%s\n' "$star" | grep -v '^[[:space:]]*#' | sed -e 's/\\"//g' -e "s/\\\\'//g" -e 's/"[^"]*"//g' -e "s/'[^']*'//g" \
            | grep -qE '(^|[;&|(){}[:space:]])setup_fail([[:space:]]|$)' || bad "$d" "R4 the '*)' branch does not call setup_fail (as a command, not inside a string)"
    fi
    # R5 / R6 (body lines only)
    body=$(printf '%s\n' "$blk" | awk -F'\t' '$1=="B"{print $2}')
    if printf '%s\n' "$body" | grep -qE '^[[:space:]]*[A-Za-z_][A-Za-z0-9_]*\(\)[[:space:]]*\{'; then
        bad "$d" "R5 a function is defined inside the ARM case (r9d-nonvacuity extracts by function and would see nothing)"
    fi
    # The word "exit" is also an English word every proxy drill uses in its claim texts ("SS via exit agt2
    # flows"), so quoted strings are blanked before the command-position match: escaped quotes first (so a
    # `sh -c "! \"$SIM\" …"` does not break the pairing), then whole "…" and '…' spans. A bare `exit` as a
    # COMMAND is never inside quotes, and a string that swallows one across a line-continuation is exactly the
    # kind of line (a description) that cannot carry a command anyway.
    # The trailing class admits every terminator a shell allows after a command word — `exit;;`, `exit;`,
    # `exit)`, `exit &`, `exit |` — not only whitespace/EOL (round-2 R3-F8: `|| exit;;` passed).
    if printf '%s\n' "$body" | grep -v '^[[:space:]]*#' | sed -e 's/\\"//g' -e "s/\\\\'//g" -e 's/"[^"]*"//g' -e "s/'[^']*'//g" \
        | grep -qE '(^|[;&|(){}[:space:]])exit([[:space:];&|)}]|$)'; then
        bad "$d" "R6 a bare 'exit' inside the ARM case (ends the drill with no verdict line; use setup_fail/die)"
    fi
    # R7
    if grep -q '^_gap_drill_level()' "$f"; then
        for a in $arms; do
            branch=$(printf '%s\n' "$blk" | awk -F'\t' -v a="$a" '$1=="L"{cur=$2; next} cur==a && $1=="B"{print $2}')
            printf '%s\n' "$branch" | grep -v '^[[:space:]]*#' | grep -q '_gap_drill_level' \
                || bad "$d" "R7 arm $a does not call _gap_drill_level (the drill-level gap must be counted by every arm)"
        done
    fi
    # R8
    esac_line=$(awk '
        function count(s, re,   n) { n = 0; while (match(s, re)) { n++; s = substr(s, RSTART + RLENGTH) } return n }
        BEGIN { RE_OPEN = "(^|[;&|(){} \t])case[ \t]"; RE_CLOSE = "(^|[;&|(){} \t])esac([ \t;]|$)" }
        /^[ \t]*case "\$\{ARM:\?\}" in[ \t]*$/ && !s { s = 1; depth = 1; next }
        s { line = $0
            if (depth == 1 && match(line, /^[ \t]*[A-Za-z0-9*|]+\)/)) line = substr(line, RLENGTH + 1)
            code = line; if (code ~ /^[ \t]*#/) code = ""; else sub(/[ \t]#.*$/, "", code)
            depth += count(code, RE_OPEN) - count(code, RE_CLOSE)
            if (depth <= 0) { print NR; exit } }' "$f")
    if [ -n "$esac_line" ]; then
        sed -n "$((esac_line + 1)),\$p" "$f" | grep -q '^[[:space:]]*drill_end' || bad "$d" "R8 no drill_end after the top-level esac"
    fi
done

if [ "$FAIL" != 0 ]; then
    printf 'arm-manifest-lint: %s problem(s)\n' "$FAIL" >&2
    exit 1
fi
n=$(for f in "$DRILLS"/*.sh; do [ -e "$f" ] && manifest_has "$f" && echo x; done | wc -l | tr -d ' ')
printf 'arm-manifest-lint: OK (%s arm-split drill(s) well-formed, the rest manifest-free)\n' "$n"
