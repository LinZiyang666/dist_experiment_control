#!/bin/sh
# assert-identity.sh — the IDENTITY companion of kept-sites.sh. POSIX sh (sh -n + dash -n clean).
#
#     cd test/simcluster && sh tests/assert-identity.sh                          # print  drill<TAB>primitive<TAB>desc
#     cd test/simcluster && sh tests/assert-identity.sh --check base.tsv        # fail on ANY per-drill difference
#
# WHAT IT RECORDS. For every executable call site of the six claim primitives (the same tokenizer rule as
# kept-sites.sh: whole-line comments dropped, keyword at a COMMAND POSITION), the primitive and the FIRST
# quoted string that follows it — the assertion's description, taken as SOURCE TEXT ($SID and friends
# unexpanded). One row per site, so the file is a multiset: two identical rows are two sites.
#
# WHY A SECOND GATE. kept-sites.sh counts sites, and a count cannot see a swap: delete `assert_refuses
# "force-single refuses while a peer is alive"` and add `assert_ok "cluster status prints"` elsewhere in the
# same drill and the count is unchanged, the gate green, and a refusal the product must keep has quietly
# become a print check (testing-standards G4: compare identity multisets, never sizes). Splitting a drill
# into arms (simcluster-speed D) is exactly the kind of edit that moves every assertion in a file; this is
# the receipt that the CLAIMS moved and none was dropped or reworded on the way (plan §6.1 receipt 1).
#
# BASELINE POLICY: --check demands EQUALITY per drill — rows missing from the live tree and rows the baseline
# does not know are both reported. An intentional change (an arm split, a re-worded description, a claim
# retired with a stated reason) is made by regenerating the baseline in the SAME change and saying why in
# the plan/review that carries it; the internal review's claim-preservation lane reads exactly that diff.
# origin: simcluster-speed plan §5.2 M0 / §6.1.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SIM_ROOT="$(cd "$HERE/.." && pwd)"
DRILLS="${DRILLS:-$SIM_ROOT/drills}"
. "$SIM_ROOT/lib/manifest.sh"

# sites <file> [<owner-map>] : emit `owner<TAB>primitive<TAB>desc` for each call site of one drill, in
# file order. owner is the arm the site's line belongs to per the map (`<line>\t<owner>`, lib/manifest.sh
# manifest_line_arms) and empty when no map is given.
sites() {
    _st_map="${2:-/dev/null}"
    awk -v mapfile="$_st_map" '
        BEGIN {
            split("assert_ok assert_refuses assert_setup assert_bug product_red not_covered", a, " ")
            for (i in a) K[a[i]] = 1
        }
        # The owner map is read first, matched by NAME (an empty /dev/null map yields no records, so an
        # FNR==NR test would swallow the drill itself).
        FILENAME == mapfile { owner[$1] = $2; next }
        /^[ \t]*#/ { next }
        {
            line = $0
            # Split on separators that sit OUTSIDE quotes by walking the line once, keeping BOTH the original
            # fragment (the description is read back out of it) and a quote-masked twin (quoted spans → Q),
            # which is what the blind-shape check below looks at so that prose mentioning a primitive inside
            # a log/echo string can never trip it.
            n = length(line); inq = ""; frag = ""; mfrag = ""; nf = 0
            for (p = 1; p <= n; p++) {
                c = substr(line, p, 1)
                if (inq != "") {
                    frag = frag c
                    if (c == "\\" && inq == "\"") { p++; frag = frag substr(line, p, 1); continue }
                    if (c == inq) { inq = ""; mfrag = mfrag "Q" }
                    continue
                }
                if (c == "\"" || c == "\047") { inq = c; frag = frag c; continue }
                if (c ~ /[;&|(){}]/) { parts[++nf] = frag; mparts[nf] = mfrag; frag = ""; mfrag = ""; continue }
                frag = frag c; mfrag = mfrag c
            }
            parts[++nf] = frag; mparts[nf] = mfrag
            for (i = 1; i <= nf; i++) {
                w = parts[i]; mw = mparts[i]
                sub(/^[ \t]*/, "", w); sub(/^[ \t]*/, "", mw)
                # Leading control words are transparent: `if assert_ok …`, `elif ! assert_ok …`, `while`,
                # `until` and `!` all still RUN the primitive, so the claim is real and must be seen.
                while (w ~ /^(then|else|do|if|elif|while|until|!)[ \t]+/) { sub(/^[a-z!]+[ \t]+/, "", w); sub(/^[a-z!]+[ \t]+/, "", mw) }
                if (!match(w, /^[A-Za-z_][A-Za-z0-9_]*/)) continue
                kw = substr(w, 1, RLENGTH)
                if (!(kw in K)) {
                    # BLIND SHAPES (internal review round 1 R3-8): a primitive reached through a wrapper —
                    # `FOO=x assert_ok …`, `eval "assert_ok …"`, `command assert_ok …`, `sh -c "assert_ok …"`,
                    # `env … assert_ok …` — still records a claim at run time but is invisible to this file
                    # and to kept-sites. Teaching the tokenizer each wrapper only invites the next one, so
                    # the rule is SEE IT AND FAIL: the site must be rewritten as a line-leading call. The
                    # masked twin catches the unquoted wrappers; the quoting wrappers (eval / sh -c / bash -c)
                    # are recognised by their first word and checked on the ORIGINAL text.
                    blind = 0
                    if (mw ~ /^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*[ \t]+/ || kw == "command" || kw == "env" || kw == "exec" || kw == "time" || kw == "nohup" || kw == "timeout" || kw == "xargs") {
                        if (mw ~ /(^|[ \t])(assert_ok|assert_refuses|assert_setup|assert_bug|product_red|not_covered)([ \t]|$)/) blind = 1
                    }
                    if (kw == "eval" || kw == "sh" || kw == "bash") {
                        if (w ~ /(^|[ \t"\047])(assert_ok|assert_refuses|assert_setup|assert_bug|product_red|not_covered)([ \t"\047]|$)/) blind = 1
                    }
                    if (blind) printf "%s\tBLIND\t%s\n", (FNR in owner ? owner[FNR] : ""), w
                    continue
                }
                rest = substr(w, RLENGTH + 1)
                sub(/^[ \t]*/, "", rest)
                desc = ""
                q = substr(rest, 1, 1)
                if (q == "\"") {
                    # first unescaped double quote closes it; an unterminated span (continued on the next
                    # line) is recorded as-is with a trailing marker so it is still a stable identity
                    j = 2; d = ""
                    while (j <= length(rest)) {
                        c = substr(rest, j, 1)
                        if (c == "\\") { d = d c substr(rest, j + 1, 1); j += 2; continue }
                        if (c == "\"") break
                        d = d c; j++
                    }
                    desc = (j > length(rest)) ? d "<unterminated>" : d
                } else if (q == "\047") {
                    j = index(substr(rest, 2), "\047")
                    desc = (j == 0) ? substr(rest, 2) "<unterminated>" : substr(rest, 2, j - 1)
                } else {
                    # No literal description. A VARIABLE there (`_claim() { assert_ok "$1" "$2"; }` —
                    # a drill-defined wrapper) means the real claims live where the wrapper is called,
                    # which this file cannot see: N claims would collapse into one row and kept-sites
                    # would count one site (round-2 review R3-F9). Same rule as the other wrappers —
                    # SEE IT AND FAIL: the text of a claim must be literal at the call site. A bare word
                    # that is not a variable is recorded so it is never silently invisible.
                    match(rest, /^[^ \t]*/); first = substr(rest, 1, RLENGTH)
                    if (first ~ /^\$/) { printf "%s\tBLIND\t%s\n", (FNR in owner ? owner[FNR] : ""), w; continue }
                    desc = "<" first ">"
                }
                # A quoted description that is NOTHING BUT one variable (`"$1"`, `"$desc"`, `"${d}"`) is the
                # wrapper shape in its quoted form — same refusal. A literal text that merely CONTAINS a
                # variable (`"session $SID + ctl login"`) is a real, stable identity and passes.
                if (desc ~ /^\$\{?[A-Za-z_0-9@*#?]+\}?$/) { printf "%s\tBLIND\t%s\n", (FNR in owner ? owner[FNR] : ""), w; continue }
                printf "%s\t%s\t%s\n", (FNR in owner ? owner[FNR] : ""), kw, desc
            }
        }
    ' "$_st_map" "$1"
}

report_raw() {
    _rr_map="${TMPDIR:-/tmp}/assert-identity.map.$$"
    for f in "$DRILLS"/*.sh; do
        d=$(basename "$f" .sh)
        if manifest_has "$f"; then
            # ARM KEYS (plan X7 / §5.5; round-2 review R1-F2 / R3-F3): an arm-split drill's rows are keyed
            # `<drill>.<arm>` / `<drill>._shared`, so a claim that migrates to a sibling arm — same text, same
            # primitive, same file — is a row missing from one key and unknown under another, not a no-op.
            manifest_line_arms "$f" > "$_rr_map"
            sites "$f" "$_rr_map" | awk -F'\t' -v d="$d" '{ printf "%s.%s\t%s\t%s\n", d, $1, $2, $3 }'
        else
            sites "$f" | awk -F'\t' -v d="$d" '{ printf "%s\t%s\t%s\n", d, $2, $3 }'
        fi
    done
    rm -f "$_rr_map"
    # drills/lib/*.sh claims (setup-forcesingle.sh: executed by every setup_forcesingle_n2 drill), keyed
    # `lib/<name>` — same reason and same key shape as kept-sites.sh (internal review round 1 R3-1).
    for f in "$DRILLS"/lib/*.sh; do
        [ -e "$f" ] || continue
        sites "$f" | awk -F'\t' -v d="lib/$(basename "$f" .sh)" '{ printf "%s\t%s\t%s\n", d, $2, $3 }'
    done
}

# report: the identity rows, or a loud exit 3 when any drill reaches a primitive through a shape this
# file cannot attribute (BLIND rows from sites()). Both the plain listing and --check go through here, so
# a blind site can neither be baselined nor pass the check.
report() {
    _rows="${TMPDIR:-/tmp}/assert-identity.rows.$$"
    report_raw > "$_rows"
    if grep -q "$(printf '\tBLIND\t')" "$_rows"; then
        printf 'assert-identity: claim site(s) reached through a wrapper are INVISIBLE to the identity and kept-sites gates — rewrite each as a line-leading call:\n' >&2
        grep "$(printf '\tBLIND\t')" "$_rows" | awk -F'\t' '{ printf "  %s: %s\n", $1, $3 }' >&2
        rm -f "$_rows"
        exit 3
    fi
    cat "$_rows"
    rm -f "$_rows"
}

case "${1:-}" in
    "")
        report
        ;;
    --check)
        BASE="${2:-}"
        [ -n "$BASE" ] || { printf 'assert-identity: --check needs a baseline .tsv path\n' >&2; exit 2; }
        [ -f "$BASE" ] || { printf 'assert-identity: baseline not found: %s\n' "$BASE" >&2; exit 2; }
        TMP="${TMPDIR:-/tmp}/assert-identity.$$"
        mkdir -p "$TMP"
        # Not `report | sort`: a BLIND exit inside a pipeline would be swallowed by the subshell and read
        # as "every baseline row missing" — a red with the wrong reason. Fail on report's own status first.
        report > "$TMP/live.raw" || { rc=$?; rm -rf "$TMP"; exit "$rc"; }
        sort "$TMP/live.raw" > "$TMP/live"
        grep -v '^#' "$BASE" | grep -v '^$' | sort > "$TMP/base"
        # Multiset difference both ways: `comm` on sorted files keeps duplicates as distinct lines.
        comm -23 "$TMP/base" "$TMP/live" > "$TMP/missing"
        comm -13 "$TMP/base" "$TMP/live" > "$TMP/unknown"
        RC=0
        if [ -s "$TMP/missing" ]; then
            printf 'assert-identity: %s claim(s) in the baseline are NOT in the live tree (dropped or reworded):\n' "$(wc -l < "$TMP/missing")" >&2
            sed 's/^/  - /' "$TMP/missing" >&2
            RC=1
        fi
        if [ -s "$TMP/unknown" ]; then
            printf 'assert-identity: %s claim(s) in the live tree are NOT in the baseline (new or reworded):\n' "$(wc -l < "$TMP/unknown")" >&2
            sed 's/^/  + /' "$TMP/unknown" >&2
            RC=1
        fi
        rm -rf "$TMP"
        if [ "$RC" != 0 ]; then
            printf 'assert-identity: the per-drill claim multiset moved vs %s — regenerate the baseline in the same change and say why\n' "$BASE" >&2
            exit 1
        fi
        printf 'assert-identity: OK — every drill asks exactly the claims in %s\n' "$BASE"
        ;;
    *)
        printf 'usage: %s [--check base.tsv]\n' "$0" >&2
        exit 2
        ;;
esac
