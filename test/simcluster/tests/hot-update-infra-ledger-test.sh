#!/bin/sh
# Pure source/receipt custody audit. Pending declarations are never live proof.
# origin: hot-update S9 plan §9.3 / §9.4 / P37 / T87 / M34
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
[ "$#" -le 1 ] || { echo "usage: hot-update-infra-ledger-test.sh [all|controls|check|coverage]" >&2; exit 2; }
. "$HERE/../lib/supervisor.sh"
LEDGER_SUPERVISOR_DIR=
trap '[ -z "$LEDGER_SUPERVISOR_DIR" ] || rm -rf "$LEDGER_SUPERVISOR_DIR"' EXIT
if [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ]; then
    LEDGER_SUPERVISOR_DIR=$(mktemp -d)
    SIM_UPGRADE_SUPERVISOR=$(sim_supervisor_build "$(cd "$HERE/../../.." && pwd)" "$LEDGER_SUPERVISOR_DIR")
    export SIM_UPGRADE_SUPERVISOR
fi
[ -x "$SIM_UPGRADE_SUPERVISOR" ] || { echo "infra ledger tests: shared supervisor is required" >&2; exit 2; }
"$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B - "$HERE/.." "${1:-all}" <<'PYLEDGER'
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import re
import sys
import tempfile
import types

ROOT = Path(sys.argv[1]).resolve()
HEADER = 'candidate_sha arm claim status blocked_by legacy_owner marginal_seconds reader_digest receipt'.split()
DRILL = types.ModuleType('infra_drill_controls')
path = ROOT / 'tests/hot-update-infra-drill-test.sh'
source = path.read_text().split("<<'PYDRILL'\n", 1)[1].rsplit('\nPYDRILL', 1)[0]
exec(compile(source, str(path), 'exec'), DRILL.__dict__)
require = DRILL.require
ARMS = [name + suffix for name, manifest in DRILL.MANIFESTS.items()
        for suffix in [''] + ['.' + arm for arm in manifest['arms'].split()]]
ARMS += ['39-hot-update-legacy-n1.NI', '39-hot-update-legacy-n1.CI']
CLAIMS = {
    '85-hot-update-nats': 'native-parent-join',
    '85-hot-update-nats.A': 'single-original-cohort-current-store',
    '85-hot-update-nats.A1': 'sole-voter-original-cohort-current-store',
    '85-hot-update-nats.B': 'n3-original-cohort-nine-hop-cycle',
    '85-hot-update-nats.D': 'n2-refusal-and-fresh-repair',
    '85-hot-update-nats.Q': 'quorum-refusal-and-fresh-repair',
    '85-hot-update-nats.L': 'lag-refusal-and-fresh-repair',
    '85-hot-update-nats.S': 'store-double-open-refusal',
    '85-hot-update-nats.J': 'exact-release-temporary-broker-join',
    '85-hot-update-nats.F': 'post-ldm-halt-and-recover',
    '86-hot-update-caddy': 'native-parent-join',
    '86-hot-update-caddy.R': 'reload-original-business',
    '86-hot-update-caddy.B': 'n3-adjacent-binary-cycle',
    '86-hot-update-caddy.I': 'root-retrofit-current-acme',
    '86-hot-update-caddy.H': 'healthy-n2-caddy-cycle',
    '86-hot-update-caddy.C': 'bad-config-cert-before-effect',
    '86-hot-update-caddy.K': 'privilege-late-upgrade-closure',
    '86-hot-update-caddy.F': 'shadow-survives-controller-failure-recover',
    '86-hot-update-caddy.U': 'support-warm-adjacent-cycle',
    '39-hot-update-legacy-n1.NI': 'legacy-nats-capability-safe-refusal',
    '39-hot-update-legacy-n1.CI': 'legacy-caddy-capability-safe-refusal',
}
# Refusal and same-PID reload arms preserve the running binary by design. Only
# a claim that promises an adjacent binary cycle requires two distinct images.
BINARY_CYCLES = {name + '.' + arm for name, arms in (
    ('85-hot-update-nats', ('A', 'A1', 'B')), ('86-hot-update-caddy', ('B', 'U')),
) for arm in arms}
FILES = ['hot-update-infra-ledger.tsv', 'expected-verdicts.tsv', 'expected-verdicts-log.md',
         'tests/assert-identity.baseline.tsv', 'tests/kept-sites.baseline.tsv', 'drill-costs.tsv']


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate receipt field: ' + key)
        result[key] = value
    return result


def contained(root, name):
    require(isinstance(name, str) and name not in ('', '-'), 'missing archive path')
    relative = Path(name)
    require(not relative.is_absolute() and '..' not in relative.parts, 'archive path escapes')
    path = root / relative
    # Even a same-content link breaks custody: every ancestor and the file must
    # belong to this archive, rather than trusting resolve() of caller inputs.
    require(not any(part.is_symlink() for part in [path, *path.parents] if part != root.parent), 'archive symlink')
    require(path.resolve().is_relative_to(root.resolve()) and path.is_file(), 'missing archive file')
    return path


def sha(value):
    return isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value) is not None


def check_receipt(row, root):
    candidate, arm, claim, status, blocked, owner, marginal, reader, receipt = row
    if status == 'NOT_RUN':
        require(blocked != '-' and all(value == '-' for value in (candidate, marginal, reader, receipt)),
                'NOT_RUN contains claimed evidence or lacks blockers')
        return
    require(status in ('PASS', 'FAIL', 'INCOMPLETE'), 'unknown ledger status')
    require(re.fullmatch('[a-f0-9]{40}|[a-f0-9]{64}', candidate) is not None and sha(reader), 'missing source/reader SHA')
    require(re.fullmatch(r'[0-9]+(?:\.[0-9]+)?', marginal) is not None, 'cost is not measured')
    path = contained(root, receipt)
    value = json.loads(path.read_text(), object_pairs_hook=unique_object)
    require(isinstance(value, dict), 'receipt must be an object')
    for key, expected in (('source_sha', candidate), ('test_or_arm', arm), ('claim', claim), ('reader_digest', reader)):
        require(value.get(key) == expected, 'receipt identity mismatch: ' + key)
    for key in ('artifact_sha', 'previous_sha', 'running_sha', 'mutation_digest'):
        require(sha(value.get(key)), 'missing artifact/running/mutation identity: ' + key)
    require(value['running_sha'] == value['artifact_sha'], 'disk-only image cannot sign current runtime')
    if arm in BINARY_CYCLES:
        require(value['previous_sha'] != value['artifact_sha'], 'identical previous image cannot sign adjacent cycle')
    for key in ('pid', 'startticks'):
        require(type(value.get(key)) is int and value[key] > 0, 'missing original process identity: ' + key)
    for key in ('command', 'log_mapping', 'phase_intersections', 'oracle_inputs'):
        require(bool(value.get(key)), 'empty receipt: ' + key)
    wall = value.get('wall_seconds')
    require(type(wall) in (int, float) and math.isfinite(wall) and wall >= 0 and float(marginal) == wall,
            'cost differs from measured native wall time')
    contained(path.parent, value.get('stdout_path'))
    stderr = contained(path.parent, value.get('stderr_path'))
    # Judge archived raw native bytes, not a JSON verdict manufactured by the
    # collector. Product rc75 has no drill meaning and never becomes GREEN.
    verdict, counters = DRILL.native(stderr.read_text(), value.get('native_rc'))
    require(value.get('verdict') == verdict, 'receipt translates raw native verdict')
    keys = ['assert_fail', 'setup_red', 'product_red', 'not_covered', 'nc_gap', 'nc_guard', 'pass']
    require(value.get('counters') == dict(zip(keys, counters)) and
            all(type(item) is int for item in value['counters'].values()), 'counter custody mismatch')
    if '.' in arm:
        drill, label = arm.rsplit('.', 1)
        # An unmatched extra marker is malformed evidence, not unrelated log
        # output. This mirrors the native verdict's exact-one-line rule.
        # origin: hot-update S9 plan §9.3 / T87 / M34
        require(re.findall(r'^DRILL-ARM arm=([A-Za-z0-9]+) of=([A-Za-z0-9-]+)$', stderr.read_text(), re.M)
                == [(label, drill)] and len(re.findall(r'^DRILL-ARM\b', stderr.read_text(), re.M)) == 1,
                'missing/duplicate/foreign native arm')
    require(isinstance(value['oracle_inputs'], list), 'oracle inputs must be a list')
    seen = set()
    for oracle in value['oracle_inputs']:
        require(isinstance(oracle, dict) and set(oracle) == {'path', 'sha256'}, 'oracle needs path and SHA256')
        require(oracle['path'] not in seen, 'duplicate oracle input')
        seen.add(oracle['path'])
        require(digest(contained(path.parent, oracle['path'])) == oracle['sha256'], 'stale oracle input')
    require(value['stderr_path'] in seen and value['stdout_path'] in seen, 'raw outputs lack digests')
    if status == 'PASS':
        require(blocked == '-' and verdict == 'GREEN', 'positive claim signed by native red')
    else:
        require(blocked != '-' and verdict != 'GREEN' and
                (status != 'INCOMPLETE' or verdict == 'INCOMPLETE'), 'red claim lost native blocker')


def rows(text, width):
    result = [line.split('\t') for line in text.splitlines() if line and not line.startswith('#')]
    require(all(len(row) == width and all(row) for row in result), 'ledger row width or empty cell')
    return result


def parent_claims(ledger):
    require(Counter(row[1] for row in ledger) == Counter(ARMS), 'missing/duplicate/unowned infra arm')
    require(all(row[2] == CLAIMS[row[1]] for row in ledger), 'infra arm lost its planned claim')
    require(len({row[0] for row in ledger if row[3] != 'NOT_RUN'}) <= 1, 'mixed candidate evidence')
    by_arm = {row[1]: row for row in ledger}
    for name, manifest in DRILL.MANIFESTS.items():
        if by_arm[name][3] == 'PASS':
            require(all(by_arm[name + '.' + arm][3] == 'PASS' for arm in manifest['arms'].split()),
                    'positive parent hides red/pending child')


# origin: S9 internal review ledgers4-b F2 / hot-update S9 plan §9.3
def coverage_inventory(text):
    for name, manifest in DRILL.MANIFESTS.items():
        matches = [line.split('|')[1:-1] for line in text.splitlines()
                   if line.startswith('| ' + name + ' |')]
        require(len(matches) == 1, 'coverage inventory omits or duplicates infra drill: ' + name)
        cells = [cell.strip() for cell in matches[0]]
        require(len(cells) == 4 and cells[1].split() == manifest['arms'].split(),
                'coverage inventory changes infra arms: ' + name)
        require(cells[3] == 'DECLARED / NOT_RUN', 'coverage inventory claims unobserved infra evidence: ' + name)


# origin: S9 internal review ledgers4-b F2 / hot-update S9 plan §9.3
def coverage_controls():
    good = '\n'.join('| ' + name + ' | ' + manifest['arms'] +
                     ' | synthetic declaration control | DECLARED / NOT_RUN |'
                     for name, manifest in DRILL.MANIFESTS.items()) + '\n'
    coverage_inventory(good)
    count = 0
    for line in good.splitlines(keepends=True):
        cells = line.split('|')
        for bad in (good.replace(line, ''), good + line,
                    good.replace(line, line.replace(cells[2], ' omitted ', 1)),
                    good.replace(line, line.replace('DECLARED / NOT_RUN', 'OBSERVED / GREEN rc0')),
                    good.replace(line, line.replace('| synthetic declaration control ', ''))):
            try:
                coverage_inventory(bad)
            except ValueError:
                count += 1
                continue
            raise ValueError('coverage inventory mutation survived: ' + cells[1].strip())
    print('infra coverage controls: PASS (%d missing/duplicate/arm/status/schema mutations)' % count)


def check():
    ledger = rows((ROOT / FILES[0]).read_text(), len(HEADER))
    require(ledger.pop(0) == HEADER, 'infra ledger schema changed')
    parent_claims(ledger)
    for row in ledger:
        require(row[5] == '-', 'ordinary infra control mislabelled historical loss')
        check_receipt(row, ROOT)
    expected = DRILL.table((ROOT / 'expected-verdicts.tsv').read_text())
    notes = (ROOT / 'expected-verdicts-log.md').read_text()
    costs = rows((ROOT / 'drill-costs.tsv').read_text(), 4)
    for name in ARMS:
        require(expected[name] == [name, 'GREEN', '0', '-', '-', name], 'positive native expectation changed: ' + name)
        require(notes.count('## ' + name + '\n') == 1, 'missing/duplicate note-ref: ' + name)
        if '.' in name:
            cost, = [row for row in costs if row[0] == name]
            require(cost[1].isdigit() and int(cost[1]) > 0, 'empty cost')
            if 'NOT_RUN' in cost[3]:
                require('estimate' in cost[3], 'estimate disguised as measured cost')
            else:
                require('COMPLETE' in cost[3] and 'image ' in cost[3] and 'estimate' not in cost[3], 'unbound measured cost')
    coverage = (ROOT.parents[1] / 'docs/reviews/simcluster-coverage-inventory.md').read_text()
    for name in DRILL.MANIFESTS:
        require(name in coverage, 'coverage inventory omits infra drill')
    coverage_inventory(coverage)
    # These are the actual claim predicates, not a plan-derived count ledger.
    for script, baseline in (('assert-identity', 'assert-identity.baseline.tsv'), ('kept-sites', 'kept-sites.baseline.tsv')):
        result = DRILL.command(['sh', ROOT / ('tests/' + script + '.sh'), '--check', ROOT / ('tests/' + baseline)])
        require(result.returncode == 0, result.stdout + result.stderr)
    DRILL.nats_manifest()
    DRILL.caddy_manifest()
    DRILL.regression_scope()


def controls():
    # Synthetic archives establish guard behaviour only. Every mutation edits
    # real temporary bytes and goes through the same receipt reader as check().
    with tempfile.TemporaryDirectory(prefix='infra-ledger-controls-') as directory:
        root = Path(directory)
        raw = ('DRILL-ARM arm=A of=85-hot-update-nats\n'
               'DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 '
               'not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- synthetic control\n')
        (root / 'stderr.raw').write_text(raw)
        (root / 'stdout.raw').write_text('synthetic oracle control\n')
        row = ['a' * 40, ARMS[1], CLAIMS[ARMS[1]], 'PASS', '-', '-', '1.25', 'e' * 64, 'receipt.json']
        value = dict(source_sha=row[0], test_or_arm=row[1], claim=row[2], reader_digest=row[7],
                     artifact_sha='b' * 64, previous_sha='c' * 64, running_sha='b' * 64, mutation_digest='d' * 64,
                     pid=1, startticks=1, wall_seconds=1.25, command='synthetic control, never product execution',
                     log_mapping=['stdout.raw', 'stderr.raw'], phase_intersections=['synthetic'],
                     oracle_inputs=[dict(path=name, sha256=digest(root / name)) for name in ('stdout.raw', 'stderr.raw')],
                     stdout_path='stdout.raw', stderr_path='stderr.raw', native_rc=0, verdict='GREEN',
                     counters={'assert_fail': 0, 'setup_red': 0, 'product_red': 0, 'not_covered': 0,
                               'nc_gap': 0, 'nc_guard': 0, 'pass': 1})
        receipt = root / 'receipt.json'
        receipt.write_text(json.dumps(value))
        check_receipt(row, root)
        # Reload and honest legacy refusal carry the same executable bytes.
        # Requiring a binary flip here would make their true positive arms red.
        for arm in ('86-hot-update-caddy.R', '39-hot-update-legacy-n1.NI'):
            unchanged = dict(value, test_or_arm=arm, claim=CLAIMS[arm], previous_sha=value['artifact_sha'])
            drill, label = arm.rsplit('.', 1)
            (root / 'stderr.raw').write_text(raw.replace('arm=A of=85-hot-update-nats', 'arm=' + label + ' of=' + drill))
            unchanged['oracle_inputs'] = [dict(path=name, sha256=digest(root / name)) for name in ('stdout.raw', 'stderr.raw')]
            same_row = row[:]
            same_row[1:3] = [arm, CLAIMS[arm]]
            receipt.write_text(json.dumps(unchanged))
            check_receipt(same_row, root)
        (root / 'stderr.raw').write_text(raw)
        for marker in ('DRILL-ARM malformed\n', 'DRILL-ARM arm=A of=85-hot-update-nats\n'):
            (root / 'stderr.raw').write_text(raw + marker)
            changed = dict(value, oracle_inputs=[dict(path=name, sha256=digest(root / name))
                                                 for name in ('stdout.raw', 'stderr.raw')])
            receipt.write_text(json.dumps(changed))
            try:
                check_receipt(row, root)
            except ValueError:
                pass
            else:
                raise ValueError('malformed/duplicate native arm survived')
        (root / 'stderr.raw').write_text(raw)
        receipt.write_text(json.dumps(value)[:-1] + ',"source_sha":' + json.dumps(value['source_sha']) + '}')
        try:
            check_receipt(row, root)
        except ValueError:
            pass
        else:
            raise ValueError('duplicate receipt field survived')
        receipt.write_text(json.dumps(value))
        stdout = root / 'stdout.raw'
        original_stdout = stdout.read_bytes()
        (root / 'same-content.raw').write_bytes(original_stdout)
        stdout.unlink()
        stdout.symlink_to('same-content.raw')
        try:
            check_receipt(row, root)
        except ValueError:
            pass
        else:
            raise ValueError('same-content archive symlink survived')
        finally:
            stdout.unlink()
            stdout.write_bytes(original_stdout)
        mutations = []
        for key in value:
            bad = dict(value)
            del bad[key]
            mutations.append(('missing-' + key, row, bad))
        for key, replacement in (('native_rc', 75), ('native_rc', True), ('running_sha', 'f' * 64),
                                 ('previous_sha', value['artifact_sha']), ('pid', 0), ('pid', True),
                                 ('wall_seconds', float('nan')), ('wall_seconds', 2), ('verdict', 'PRODUCT-RED'),
                                 ('stderr_path', '../stderr.raw'), ('oracle_inputs', [])):
            mutations.append((key, row, dict(value, **{key: replacement})))
        for label, bad_row, bad in mutations:
            receipt.write_text(json.dumps(bad))
            try:
                check_receipt(bad_row, root)
            except (ValueError, KeyError):
                continue
            raise ValueError('receipt mutation survived: ' + label)
        receipt.write_text(json.dumps(value))
        (root / 'stdout.raw').write_text('stale input\n')
        try:
            check_receipt(row, root)
        except ValueError:
            pass
        else:
            raise ValueError('stale archived input survived')
        pending = [['-', arm, CLAIMS[arm], 'NOT_RUN', 'P39', '-', '-', '-', '-'] for arm in ARMS]
        parent_claims(pending)
        signed_parent = [list(item) for item in pending]
        signed_parent[0] = row[:]
        signed_parent[0][1] = ARMS[0]
        signed_parent[0][2] = CLAIMS[ARMS[0]]
        unowned_claim = [list(item) for item in pending]
        unowned_claim[1][2] = 'disk-only-replacement'
        mixed = [list(item) for item in pending]
        for index, candidate in ((1, 'a' * 40), (2, 'b' * 40)):
            mixed[index][0], mixed[index][3] = candidate, 'FAIL'
        for bad in (pending[:-1], pending + pending[-1:], signed_parent, mixed, unowned_claim):
            try:
                parent_claims(bad)
            except ValueError:
                continue
            raise ValueError('missing child/false-green parent survived')
        for original in pending:
            check_receipt(original, root)
        bad = pending[0][:]
        bad[0] = 'a' * 40
        try:
            check_receipt(bad, root)
        except ValueError:
            pass
        else:
            raise ValueError('NOT_RUN fabricated evidence survived')
    print('infra ledger controls: PASS (%d receipt mutations, stale raw input, missing/duplicate child, pending parent)' % len(mutations))


def main():
    selection = sys.argv[2]
    require(selection in ('all', 'controls', 'check', 'coverage'), 'unknown infra ledger selection')
    if selection == 'coverage':
        coverage_inventory((ROOT.parents[1] / 'docs/reviews/simcluster-coverage-inventory.md').read_text())
        print('infra coverage inventory: PASS (declared arms only; live evidence NOT_RUN)')
    if selection in ('all', 'controls'):
        controls()
        coverage_controls()
    if selection in ('all', 'check'):
        missing = [name for name in FILES if not (ROOT / name).is_file()]
        if missing:
            print('NOT_RUN pending P36: ' + ', '.join(missing), file=sys.stderr)
            return 2
        check()
        print('infra ledger: PASS (source/receipt integrity; live claims retain native status)')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, KeyError, ValueError, TypeError) as error:
        print('infra ledger tests: ' + str(error), file=sys.stderr)
        sys.exit(1)
PYLEDGER
