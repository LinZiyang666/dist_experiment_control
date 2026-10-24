# lib/manifest.sh — the ARM MANIFEST reader. POSIX sh (sh -n + dash -n clean). Sourced by `simcluster`
# (the `drill --arm` verb) and by run-drills.sh (unit expansion, lanes, per-unit timeouts), and driven
# directly by tests/arm-manifest-lint.sh — ONE parser, so the drill, the runner and the lint cannot
# disagree about what a manifest says. Pure functions: no side effects, no globals written.
#
# THE MANIFEST is a block of whole-line comments at the top of a drill (lint-drills.sh's code() strips
# them, so the 13 line-global drill rules never see it):
#
#     # arms: A D F
#     # fixture: A=N3-live D=N3-live F=N3-live
#     # grows: A=2 D=2 F=2
#     # worst: A=1350 D=1500 F=800
#     # forgoes: A=- D="71-minority-commit: post-A context (brk2 killed+restarted by A)" F="grow-attempts: heal residue"
#
#   arms      the execution units of this drill; `simcluster drill <name> --arm <A>` runs exactly one,
#             on instance drill-<name>-<A>, and the drill's `case "${ARM:?}" in` selects the branch.
#   fixture   what the arm builds before its claims, from a CLOSED vocabulary (manifest_fixture_ok):
#             N1 | install | N2-live | N3-live | FS-N2 | N2-cap3g | grow-claim. `-tpl` variants are added
#             the day template clusters land (plan §5.7), not before.
#   grows     how many `tether cluster add` grows the arm performs; the runner's grow lane releases a
#             unit's slot once that many grow-done markers have been written (or the unit exits). The
#             markers are written by the grow FIXTURE after its post-check when the arm grows through
#             grow_to_3 / grow_to_2 (a nuke+retry repeats the grows without adding lines), and by
#             cmd_grow per grow otherwise — drills/lib/cluster.sh, external review F4.
#   worst     the arm's declared worst-case seconds; run-drills.sh bounds the unit at 2×worst, capped by
#             --drill-timeout. Missing ⇒ the global ceiling (fail-open only in the LONGER direction).
#   forgoes   the contention/post-grow sensors this arm no longer samples now that it runs on its own
#             fresh cluster instead of after its former siblings: `-` (none) or `<id> [<id>…][: free
#             text]` — the ids before the first ':' must exist in contention-sensors.tsv
#             (tests/contention-registry-check.sh); the text after it is for the reader.
#
# A drill WITHOUT `# arms:` is one unit (its own name), lane and grows derived from its content, worst
# from the global ceiling — every pre-existing drill keeps working unchanged, and so do the 1325 lines of
# hermetic fixture drills that have no manifest at all (plan X2).
#
# Values are either a run of non-space characters or a double-quoted string with no inner double quote.
# origin: simcluster-speed plan §5.5.

# manifest_has <file> : 0 iff the drill declares `# arms:`.
manifest_has() { grep -q '^# arms:' "$1"; }

# An arm id must remain one token in both shell dispatch and TSV identities.
# Leading/trailing or doubled hyphens would alias malformed registry keys.
# origin: hot-update S8 plan §9.2 / P27 ledger request
manifest_arm_ok() { printf '%s\n' "$1" | grep -qE '^[A-Za-z0-9]+(-[A-Za-z0-9]+)*$'; }

# manifest_arms <file> : the arm names, space-separated, in declaration order (empty when no manifest).
manifest_arms() {
    sed -n 's/^# arms:[[:space:]]*//p' "$1" | head -1 | tr -s '[:space:]' ' ' | sed 's/^ //; s/ $//'
}

# manifest_value <file> <key> <arm> : the arm's value for key ∈ {fixture,grows,worst,forgoes}, quotes
# stripped; empty when the key line or the arm's token is absent.
manifest_value() {
    sed -n "s/^# $2:[[:space:]]*//p" "$1" | head -1 | awk -v arm="$3" '
        {
            s = $0
            while (length(s) > 0) {
                sub(/^[ \t]+/, "", s)
                if (!match(s, /^[A-Za-z0-9]+(-[A-Za-z0-9]+)*=/)) break
                name = substr(s, 1, RLENGTH - 1); s = substr(s, RLENGTH + 1)
                if (substr(s, 1, 1) == "\"") {
                    e = index(substr(s, 2), "\"")
                    if (e == 0) { val = substr(s, 2); s = "" } else { val = substr(s, 2, e - 1); s = substr(s, e + 2) }
                } else {
                    match(s, /^[^ \t]*/); val = substr(s, 1, RLENGTH); s = substr(s, RLENGTH + 1)
                }
                if (name == arm) { print val; exit }
            }
        }'
}

# manifest_keys_ok <file> : 0 iff every manifest line is one of the five known keys (a typo such as
# `# fixtures:` would otherwise be silently ignored — the runner would treat the arm as N1 / no worst).
manifest_keys_ok() {
    # Any `# <word>:` line whose word is a near-miss of a manifest key is a typo; exact keys pass.
    _mk_bad=$(awk '
        /^# [a-z]+:/ {
            w = $2; sub(/:$/, "", w)
            if (w == "arms" || w == "fixture" || w == "grows" || w == "worst" || w == "forgoes") next
            if (w ~ /^(arm|arms?|fixtures?|grows?|worsts?|forgoe?s?)$/) print w
        }' "$1")
    [ -z "$_mk_bad" ]
}

# manifest_fixture_ok <value> : 0 iff the value is in the closed vocabulary.
manifest_fixture_ok() {
    case "$1" in N1|install|N2-live|N3-live|FS-N2|N2-cap3g|grow-claim) return 0 ;; *) return 1 ;; esac
}

# manifest_delegated_grows <file> : arm/fixture/grows from the S8 fixture's
# independent registry. A fixture declaration alone cannot earn grow capacity:
# require the real adapter call and its registry, and fail closed if either is
# missing. This is source accounting, never proof that deployment grew a node.
# origin: hot-update S8 plan §9.2 / P27 delegated N3 ledger request
manifest_delegated_grows() {
    # S10 moved setup behind its assembly adapters. Read their independent
    # topology catalogs only when the corresponding arm dispatch remains live;
    # a comment declaring delegated growth cannot reserve a runner lane.
    # origin: S10 internal review RUNALL-B F1
    if grep -q '^# delegated-grows:' "$1" ||
        grep -v '^[[:space:]]*#' "$1" | grep -qE 'fullstack_(fixture|mutations)\.py'; then
        manifest_fullstack_grows "$1"
        return $?
    fi
    grep -v '^[[:space:]]*#' "$1" | grep -qF '"$HERE/drills/lib/upgrade_tx_arms.tsv"' || return 0
    grep -v '^[[:space:]]*#' "$1" | grep -qF 'fixture.start(root, manifest, reader)' || return 1
    _md_registry="$(dirname "$1")/lib/upgrade_tx_arms.tsv"
    [ -f "$_md_registry" ] || return 1
    awk -F'\t' '
        /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
        !header {
            for (i = 1; i <= NF; i++) col[$i] = i
            if (!("arm" in col) || !("fixture" in col) || !("grows" in col)) { bad = 1; exit }
            header = 1; next
        }
        {
            arm = $(col["arm"]); fx = $(col["fixture"]); g = $(col["grows"])
            if (arm !~ /^[A-Za-z0-9]+(-[A-Za-z0-9]+)*$/ || seen[arm]++ ||
                g !~ /^[0-9]+$/ || (fx != "N1" && fx != "N3-live") ||
                (fx == "N1" && g != 0) || (fx == "N3-live" && g != 2)) { bad = 1; exit }
            rows = rows arm "\t" fx "\t" g "\n"; n++
        }
        END { if (bad || !header || !n) exit 1; printf "%s", rows }
    ' "$_md_registry"
}

# manifest_fullstack_grows <file> : source accounting for the S10 adapters,
# including control arms whose single/quorum cells share one grow-lane unit.
# Catalog parsing stays read-only: it never imports or starts a deployment.
# origin: S10 internal review RUNALL-B F1
manifest_fullstack_grows() {
    python3 -I -B - "$1" <<'PY'
import json
from pathlib import Path
import re
import sys

def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate catalog member')
        result[key] = value
    return result

try:
    drill = Path(sys.argv[1])
    lines = drill.read_text().splitlines()
    declared = [line.split(':', 1)[1].strip() for line in lines if line.startswith('# delegated-grows:')]
    adapters = {'lib/fullstack_manifest.json': ('fullstack_fixture', 'run --arm', 'fullstack-fixture-v1', 'arms'),
                'lib/fullstack_mutations.json': ('fullstack_mutations', 'run-arm --mutation', 'fullstack-mutations-v1', 'mutations')}
    if len(declared) != 1 or declared[0] not in adapters:
        raise ValueError('missing or unknown delegated catalog')
    module, command, schema, field = adapters[declared[0]]
    dispatch = 'python3 -I -B "$HERE/drills/lib/' + module + '.py" ' + command + ' "$ARM"'
    if not any(line.strip() == dispatch + ' \\' for line in lines):
        raise ValueError('missing delegated arm dispatch')
    catalog = json.loads((drill.parent / declared[0]).read_text(), object_pairs_hook=unique,
                         parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite catalog')))
    if catalog['schema'] != schema or not isinstance(catalog[field], list) or not catalog[field]:
        raise ValueError('invalid delegated catalog schema')
    seen, rows = set(), []
    for row in catalog[field]:
        arm = row['id']
        if not isinstance(arm, str) or re.fullmatch(r'[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*', arm) is None or arm in seen:
            raise ValueError('invalid or duplicate delegated arm')
        seen.add(arm)
        topologies = [row['topology']] if field == 'arms' else row['topologies']
        if not isinstance(topologies, list) or not topologies or any(t not in ('single', 'quorum') for t in topologies) or len(topologies) != len(set(topologies)):
            raise ValueError('invalid delegated topology cells')
        quorum = 'quorum' in topologies
        rows.append((arm, 'N3-live' if quorum else 'N1', 2 if quorum else 0))
    required = catalog['required_children']
    if not isinstance(required, list) or required != [row[0] for row in rows]:
        raise ValueError('delegated catalog child union differs')
    for row in rows:
        print(*row, sep='\t')
except (OSError, ValueError, TypeError, KeyError, RecursionError) as error:
    print('manifest: invalid fullstack delegated growth: ' + str(error), file=sys.stderr)
    sys.exit(1)
PY
}

# manifest_grow_tokens <file> : how many grow call sites the drill's CODE (comments stripped) contains.
# The tokens are the sim's grow entry points; a drill that grows through any of them is a grow-lane
# unit (plan X2: the lane is DERIVED from content, `# fixture:` is a declaration the lint cross-checks).
manifest_grow_tokens() {
    _mg_literal=$(grep -v '^[[:space:]]*#' "$1" | grep -oE 'grow_to_3|grow_to_2|setup_forcesingle_n2|"\$SIM" grow|"\$0" grow|\$SIM grow' | wc -l | tr -d ' ')
    _mg_delegate=$(manifest_delegated_grows "$1") || return 1
    _mg_extra=$(printf '%s\n' "$_mg_delegate" | awk -F'\t' '$3 > 0 { n = 1 } END { print n + 0 }')
    printf '%s\n' "$((_mg_literal + _mg_extra))"
}

# manifest_grow_capacity <file> : how many grows the drill's CODE can perform in one attempt — the upper
# bound tests/arm-manifest-lint.sh R2 holds `# grows:` to. A `grow_to_3` site is TWO grows (brk2 then
# brk3, drills/lib/cluster.sh), the other tokens one each; a retry (grow_to_3's nuke+retry) repeats them
# but declares nothing new, and — since external review F4 — writes nothing new either: the fixture
# writes its two lines once, after the whole grow passed its post-check, so the lane's release count
# is the declared first-attempt figure in fact and not only in the declaration.
manifest_grow_capacity() {
    _mc_literal=$(grep -v '^[[:space:]]*#' "$1" | grep -oE 'grow_to_3|grow_to_2|setup_forcesingle_n2|"\$SIM" grow|"\$0" grow|\$SIM grow' \
        | awk '{ n += ($0 == "grow_to_3") ? 2 : 1 } END { print n + 0 }')
    _mc_delegate=$(manifest_delegated_grows "$1") || return 1
    _mc_extra=$(printf '%s\n' "$_mc_delegate" | awk -F'\t' '$3 > n { n = $3 } END { print n + 0 }')
    printf '%s\n' "$((_mc_literal + _mc_extra))"
}

# manifest_lane <file> : grow | N1, from content.
manifest_lane() {
    _ml_tokens=$(manifest_grow_tokens "$1") || return 1
    if [ "$_ml_tokens" -gt 0 ]; then printf 'grow'; else printf 'N1'; fi
}

# manifest_units <file> : the runner's unit names for this drill — `<name>.<arm>` per arm, or `<name>`.
manifest_units() {
    _mu_name=$(basename "$1" .sh)
    if manifest_has "$1"; then
        for _mu_a in $(manifest_arms "$1"); do printf '%s.%s\n' "$_mu_name" "$_mu_a"; done
    else
        printf '%s\n' "$_mu_name"
    fi
}

# manifest_line_arms <file> : `<line-number>\t<owner>` for EVERY physical line of an arm-split drill, where
# owner is the manifest arm whose depth-1 branch of the top-level `case "${ARM:?}" in` the line sits in,
# and `_shared` everywhere else — the fixture above the case, the `*)` branch, and whatever follows the
# esac (drill_end). Nested case/esac inside a branch is tracked per OCCURRENCE (a one-line inner case is
# common), and a branch label may carry a one-liner body, which belongs to that arm.
#
# This is the SAME block parser tests/arm-manifest-lint.sh reads the case structure with (its case_block
# prints the branch view of this walk); the claim gates read it by line so that kept-sites.sh and
# assert-identity.sh can key an arm-split drill's claims per ARM. Without that keying a claim moved from
# one arm to its sibling is invisible to both gates — the total is unchanged and the multiset is unchanged
# (round-2 review R1-F2 / R3-F3; plan X7 named the channel: "总和守卫看不见一升一降").
manifest_line_arms() {
    awk '
        function count(s, re,   n) { n = 0; while (match(s, re)) { n++; s = substr(s, RSTART + RLENGTH) } return n }
        BEGIN { RE_OPEN = "(^|[;&|(){} \t])case[ \t]"; RE_CLOSE = "(^|[;&|(){} \t])esac([ \t;]|$)"; cur = "_shared" }
        {
            line = $0
            if (!inblk && line ~ /^[ \t]*case "\$\{ARM:\?\}" in[ \t]*$/) { inblk = 1; depth = 1; printf "%d\t_shared\n", NR; next }
            if (!inblk) { printf "%d\t_shared\n", NR; next }
            if (depth == 1 && match(line, /^[ \t]*[-A-Za-z0-9*|]+\)/)) {
                lab = substr(line, 1, RLENGTH); sub(/^[ \t]*/, "", lab); sub(/\)$/, "", lab)
                cur = (lab == "*") ? "_shared" : lab
                line = substr(line, RLENGTH + 1)
            }
            printf "%d\t%s\n", NR, cur
            # a whole-line comment (column 0 included) is not code: "the case" in prose is not a case
            code = line; if (code ~ /^[ \t]*#/) code = ""; else sub(/[ \t]#.*$/, "", code)
            depth += count(code, RE_OPEN) - count(code, RE_CLOSE)
            if (depth <= 0) { inblk = 0; cur = "_shared" }
        }' "$1"
}

# manifest_shared_grow_floor <file> : the grows the code BEFORE the top-level ARM case — the fixture every
# arm runs — performs on its FIRST grow site (grow_to_3 = 2, the other tokens 1; 0 when the shared code
# grows nothing). `# grows:` is the lane's release count, so an arm may not declare fewer grows than its
# shared fixture performs on its behalf — it would be released from the lane mid-grow. The floor is the
# first site, not the sum of all shared sites: a fixture's nuke-and-retry path is a second site that runs
# only on failure and declares nothing new (see manifest_grow_capacity). The lint holds `# grows:`
# between this floor and manifest_grow_capacity's ceiling (round-2 review R3-F8).
manifest_shared_grow_floor() {
    awk '/^[ \t]*case "\$\{ARM:\?\}" in[ \t]*$/ { exit } { print }' "$1" \
        | grep -v '^[[:space:]]*#' | grep -oE 'grow_to_3|grow_to_2|setup_forcesingle_n2|"\$SIM" grow|"\$0" grow|\$SIM grow' \
        | head -1 | awk '{ n = ($0 == "grow_to_3") ? 2 : 1 } END { print n + 0 }'
}

# manifest_fullstack_ceilings <fullstack_manifest.json> <99 drill> : exact TSV
# unit/worst/ceiling/minimum rows. P19 owns the JSON's unit_ceilings array; other
# manifest sections belong to its fixture. No command from either file is run.
# A missing or malformed map must fail before preflight, cleanup, or any launch.
# origin: hot-update S10 plan §9.1–§9.2 / P32
manifest_fullstack_ceilings() {
    python3 -I -B - "$1" "$2" <<'PY'
import json
from pathlib import Path
import re
import sys

def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON member")
        result[key] = value
    return result

try:
    manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"),
                          object_pairs_hook=unique,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be an object")
    rows = manifest.get("unit_ceilings")
    if not isinstance(rows, list):
        raise ValueError("missing unit_ceilings array")
    # Frozen maxima are functional configuration, never receipt exceptions.
    # The floors are actual two-leg 1 GiB/1 MiB/s durations (§3.3), twice
    # for A/B's two cohorts. P19 can declare a higher measured minimum.
    bounds = {"C1": (2400, 4800, 2048), "C3": (2400, 4800, 2048),
              "A": (6000, 12000, 4096), "B": (6000, 12000, 4096),
              "S1": (2700, 5400, 2048), "S3": (2700, 5400, 2048),
              "F1": (2700, 5400, 2048), "F3": (2700, 5400, 2048),
              "K": (1800, 3600, 1)}
    prefix = "99-hot-update-fullstack."
    expected = {prefix + arm for arm in bounds}
    seen = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"unit", "worst_s", "ceiling_s", "minimum_s"}:
            raise ValueError("invalid ceiling row fields")
        unit = row["unit"]
        if not isinstance(unit, str) or unit not in expected or unit in seen:
            raise ValueError("unknown or duplicate ceiling unit")
        worst, ceiling, floor = bounds[unit[len(prefix):]]
        if any(type(row[key]) is not int for key in ("worst_s", "ceiling_s", "minimum_s")):
            raise ValueError("ceiling seconds must be integers")
        if (row["worst_s"], row["ceiling_s"]) != (worst, ceiling):
            raise ValueError("ceiling differs from frozen unit bounds")
        if not floor <= row["minimum_s"] <= min(2 * worst, ceiling):
            raise ValueError("ceiling minimum excludes the actual transfer duration")
        seen[unit] = row
    if set(seen) != expected:
        raise ValueError("missing ceiling unit")
    lines = Path(sys.argv[2]).read_text(encoding="utf-8").splitlines()
    arms = [line[len("# arms:"):].split() for line in lines if line.startswith("# arms:")]
    worst_lines = [line[len("# worst:"):].split() for line in lines if line.startswith("# worst:")]
    if len(arms) != 1 or len(arms[0]) != len(bounds) or set(arms[0]) != set(bounds):
        raise ValueError("drill arms differ from exact ceiling union")
    if len(worst_lines) != 1:
        raise ValueError("missing or duplicate worst header")
    worst_header = {}
    for token in worst_lines[0]:
        match = re.fullmatch(r"([A-Za-z0-9]+(?:-[A-Za-z0-9]+)*)=([1-9][0-9]*)", token)
        if match is None or match[1] in worst_header:
            raise ValueError("invalid or duplicate worst token")
        worst_header[match[1]] = int(match[2])
    if worst_header != {arm: value[0] for arm, value in bounds.items()}:
        raise ValueError("worst header differs from ceiling map")
    for unit in sorted(seen):
        row = seen[unit]
        print(unit, row["worst_s"], row["ceiling_s"], row["minimum_s"], sep="\t")
except (OSError, ValueError, TypeError, RecursionError) as error:
    print("manifest: invalid fullstack ceilings: " + str(error), file=sys.stderr)
    sys.exit(2)
PY
}
