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

# The S10 crosscheck reads the real manifest and declaration ledgers; it never
# launches a drill. Synthetic gotcha-ledger selftests retain their own scope.
# origin: hot-update S10 plan §9 / §10.2 / P31
fullstack_crosscheck() {
    python3 -I -B - "$1" <<'FULLSTACK_LEDGER_PY'
import collections
import csv
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
sim = root / 'test/simcluster'
positive = '99-hot-update-fullstack'
controls = '100-hot-update-fullstack-controls'

def require(condition, message):
    if not condition:
        raise ValueError(message)

def text(path):
    return (sim / path).read_text()

def records(raw):
    return list(csv.reader((line for line in raw.splitlines() if line and not line.startswith('#')), delimiter='\t'))

def table(raw):
    rows = records(raw)
    require(bool(rows), 'empty ledger table')
    header = rows.pop(0)
    require(len(set(header)) == len(header), 'duplicate ledger column')
    require(all(len(row) == len(header) for row in rows), 'invalid ledger column count')
    return header, [dict(zip(header, row)) for row in rows]

def keyed(rows, label):
    result = {}
    for row in rows:
        require(row[0] not in result, 'duplicate ' + label + ': ' + row[0])
        result[row[0]] = row
    return result

def header(raw, name):
    matches = re.findall(r'^# ' + name + r': (.*)$', raw, re.M)
    require(len(matches) == 1, 'missing or duplicate manifest ' + name)
    return matches[0].split()

def units():
    result = {}
    for path in sorted((sim / 'drills').glob('*.sh')):
        raw = path.read_text()
        arms = re.findall(r'^# arms: (.*)$', raw, re.M)
        if not arms:
            result[path.stem] = dict(drill=path.stem, arm='-', fixture='-', grows='-', worst_s='-', ceiling_s='-')
            continue
        children = header(raw, 'arms')
        require(len(set(children)) == len(children) and children, 'duplicate or empty arm manifest')
        fields = {}
        for name in ('fixture', 'grows', 'worst', 'forgoes'):
            pairs = [item.split('=', 1) for item in header(raw, name)]
            require(all(len(item) == 2 for item in pairs), 'malformed manifest ' + name)
            require(len(dict(pairs)) == len(pairs) and set(dict(pairs)) == set(children), 'manifest field union ' + path.stem)
            fields[name] = dict(pairs)
        for arm in children:
            worst = int(fields['worst'][arm])
            cap = 2 * worst if path.stem == positive else min(2 * worst, 2700)
            result[path.stem + '.' + arm] = dict(drill=path.stem, arm=arm, fixture=fields['fixture'][arm], grows=fields['grows'][arm], worst_s=str(worst), ceiling_s=str(cap))
    require(len(result) > 100, 'launch discovery is empty or incomplete')
    return result

try:
    launches = units()
    fixture = json.loads(text('drills/lib/fullstack_manifest.json'))
    mutations = json.loads(text('drills/lib/fullstack_mutations.json'))
    pids = 'C1 C3 A B S1 S3 F1 F3 K'.split()
    mids = 'M01A M01B M02E M02P M02F M02X M03E M03P M03F M03X M04A M04B M04P M05E M05P M05X M05R M06A M06B M06R M06F M07A M07B M07N M07C M08E M08P M08A M08B M08D M08X M08C M08O M09A M09B M10S M10C M11W M11I M11O M11P M11Q M11F M11S M11X M11D M11C M11L M11U M11K'.split()
    require(fixture['required_children'] == pids and [r['id'] for r in fixture['arms']] == pids, 'positive child union differs')
    require(mutations['required_children'] == mids and [r['id'] for r in mutations['mutations']] == mids, 'control child union differs')
    for drill, ids in ((positive, pids), (controls, mids)):
        actual = {key for key in launches if key.startswith(drill + '.')}
        require(actual == {drill + '.' + arm for arm in ids}, 'missing launch child: ' + drill)

    expected = keyed(records(text('expected-verdicts.tsv')), 'expected row')
    for drill, ids, verdict in ((positive, pids, 'GREEN'), (controls, mids, 'ASSERT-FAIL')):
        want = {drill, *(drill + '.' + arm for arm in ids)}
        require({key for key in expected if key == drill or key.startswith(drill + '.')} == want, 'missing expected child: ' + drill)
        for key in want:
            require(expected[key] == [key, verdict, '0', '-', '-', key], 'native expectation differs: ' + key)
    for arm in ('P', 'F', 'BR'):
        key = '39-hot-update-legacy-n1.' + arm
        require(expected[key][1] == 'PRODUCT-RED', 'historical native3 changed: ' + key)
    log = text('expected-verdicts-log.md')
    for key in [positive, controls, *(positive + '.' + arm for arm in pids), *(controls + '.' + arm for arm in mids)]:
        require(log.count('\n## ' + key + '\n') == 1, 'missing expectation history: ' + key)

    costs = keyed(records(text('drill-costs.tsv')), 'cost row')
    for drill in (positive, controls):
        require(drill not in costs, 'parent double counted: ' + drill)
        want = {key for key in launches if key.startswith(drill + '.')}
        require({key for key in costs if key.startswith(drill + '.')} == want, 'missing launch cost: ' + drill)
    for key, row in costs.items():
        if key.startswith((positive + '.', controls + '.')):
            require(len(row) == 4 and row[1].isdigit() and 0 < int(row[1]) <= int(launches[key]['worst_s']), 'invalid launch cost: ' + key)
            require('NOT_MEASURED' in row[3] and 'NOT_RUN' in row[3], 'unsigned launch cost missing NOT_RUN/NOT_MEASURED: ' + key)
            require('replace after' not in row[3] and 'receipt-only' not in row[3], 'cost whitelist permits functional edit: ' + key)

    ledger = text('hot-update-fullstack-ledger.tsv')
    sections = ledger.split('# launch-catalog-v1\n')
    require(len(sections) == 2, 'missing launch catalog')
    receipt_columns = re.findall(r'^# receipt-columns: (.*)$', sections[1], re.M)
    require(receipt_columns == ['candidate_id native_rc actual_wall_s peak_fd peak_rss_bytes peak_js_bytes raw_path'], 'cost whitelist contains functional columns or is missing')
    _, cells = table(sections[0])
    wanted_cells = {}
    for row in mutations['mutations']:
        for topology in row['topologies']:
            wanted_cells[row['id'], topology] = row
    seen = set()
    for row in cells:
        key = row['mutation_id'], row['topology']
        require(key in wanted_cells and key not in seen, 'unknown or duplicate mutation cell: ' + str(key))
        seen.add(key)
        source = wanted_cells[key]
        for column in ('surface', 'source_path', 'function', 'compatible_artifact_set', 'role', 'window', 'hit_event', 'expected_assertion'):
            require(row[column] == source[column], 'mutation cell source differs: ' + str(key))
        require(row['status'] == 'NOT_RUN' and row['pending_owners'] == ', '.join(source['pending_owners']), 'unsigned mutation owner differs: ' + str(key))
        require([row[c] for c in ('baseline_rc', 'mutant_native_rc', 'restore_rc')] == ['-'] * 3, 'synthetic mutation receipt: ' + str(key))
    require(seen == set(wanted_cells), 'missing mutation topology cell')

    # The catalog enumerates launch units, not parent summaries. A control's
    # cell column is the exact applicable-cell set; the preceding table holds
    # one row per cell so no topology can borrow its sibling's native chain.
    columns, catalog = table(sections[1])
    require(columns == 'regime drill arm artifact_set mutation cell required expected_native fixture grows estimate_s worst_s ceiling_s before_build_s after_build_s status pending_owner candidate_id native_rc actual_wall_s peak_fd peak_rss_bytes peak_js_bytes raw_path'.split(), 'launch catalog schema differs')
    wanted = {}
    for unit, launch in launches.items():
        regimes = ['default', 'live-grow']
        if launch['drill'] == positive:
            regimes.append('quiet')
        if launch['drill'] == controls:
            regimes.append('controls')
        artifact, mutation, cell = 'upstream-manifest', '-', launch['arm']
        if launch['drill'] == positive:
            artifact = 'tether-T0-T5+nats-adjacent+caddy-adjacent'
            cell = next(row['topology'] for row in fixture['arms'] if row['id'] == launch['arm'])
        if launch['drill'] == controls:
            source = next(row for row in mutations['mutations'] if row['id'] == launch['arm'])
            artifact, mutation, cell = source['compatible_artifact_set'], source['id'], ','.join(source['topologies'])
        require(unit in expected, 'launch missing expected verdict: ' + unit)
        native = {'GREEN': '0', 'ASSERT-FAIL': '1', 'SETUP-RED': '2', 'PRODUCT-RED': '3', 'INCOMPLETE': '4'}[expected[unit][1]]
        for regime in regimes:
            key = (regime, launch['drill'], launch['arm'], artifact, mutation, cell)
            wanted[key] = (unit, launch, native)
    seen = set()
    for row in catalog:
        key = tuple(row[column] for column in columns[:6])
        require(key not in seen, 'duplicate launch catalog key: ' + str(key))
        seen.add(key)
        require(key in wanted, 'unexpected launch catalog unit or parent: ' + str(key))
        unit, launch, native = wanted[key]
        require(row['required'] == 'yes' and row['expected_native'] == native, 'catalog native expectation differs: ' + unit)
        for column in ('fixture', 'grows', 'worst_s', 'ceiling_s'):
            require(row[column] == launch[column], 'catalog manifest differs: ' + unit + '/' + column)
        require(row['estimate_s'] == (costs[unit][1] if unit in costs else '-'), 'catalog launch cost differs: ' + unit)
        require(row['before_build_s'] == row['after_build_s'] == '-', 'unmeasured build cost')
        require(row['status'] == 'NOT_RUN' and row['pending_owner'] == ('S10/P19,P20,P26,P27,P32,P35' if launch['drill'] in (positive, controls) else 'S10/P27,P32,P35'), 'catalog pending owner differs: ' + unit)
        require(all(row[column] == '-' for column in columns[17:]), 'unsigned launch has synthetic receipt: ' + unit)
    require(seen == set(wanted), 'missing launch catalog child')

    identities = records(text('tests/assert-identity.baseline.tsv'))
    kept = keyed(records(text('tests/kept-sites.baseline.tsv')), 'kept site')
    for drill, ids, count in ((positive, pids, 4), (controls, mids, 2)):
        require(kept.get(drill) == [drill, str(count)], 'missing kept parent: ' + drill)
        require(kept.get(drill + '._shared') == [drill + '._shared', str(count)], 'missing shared kept claims: ' + drill)
        require(sum(row[0] == drill + '._shared' for row in identities) == count, 'missing assertion identities: ' + drill)
        for arm in ids:
            require(kept.get(drill + '.' + arm) == [drill + '.' + arm, '0'], 'missing kept child: ' + drill + '.' + arm)
    descriptions = {
        positive: [('assert_ok', 'fullstack required children and original cohorts satisfy the independent judges')] * 2 + [('product_red', 'original fullstack judge receipt: verdict=PRODUCT-RED rc=3'), ('not_covered', 'fullstack required live windows and upstream receipts are complete')],
        controls: [('assert_ok', 'fullstack source mutation hits its exact window and independent assertion with native 0 to 1 to 0'), ('not_covered', 'all required fullstack mutation cells have native chains')],
    }
    for drill, claims in descriptions.items():
        actual = collections.Counter(tuple(row) for row in identities if row[0].startswith(drill + '.'))
        wanted_claims = collections.Counter((drill + '._shared', primitive, description) for primitive, description in claims)
        require(actual == wanted_claims, 'assertion identity multiset differs: ' + drill)
    readme = text('README.md')
    require('functional input' in readme and 're-seal' in readme and 'S10/P35' in readme, 'cost whitelist boundary undocumented')
    print('fullstack-ledger: OK (%d launches, %d catalog rows, %d mutation cells; live evidence NOT_RUN)' % (len(launches), len(catalog), len(cells)))
except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
    print('fullstack-ledger: ' + str(error), file=sys.stderr)
    sys.exit(1)
FULLSTACK_LEDGER_PY
}
case "${1:-}" in
    --fullstack-only) fullstack_crosscheck "${2:-$REPO}"; exit $? ;;
esac
if [ "$LEDGER" = "$REPO/docs/deploy-tier-gotchas.md" ] && [ "$VERDICTS" = "$HERE/../expected-verdicts.tsv" ]; then
    fullstack_crosscheck "$REPO" || exit $?
fi


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
