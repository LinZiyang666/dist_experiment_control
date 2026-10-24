#!/bin/sh
# Read-only drill contracts and assertion-library controls; never runs a drill.
# origin: hot-update S9 plan §9.3 / §9.4 / P37 / T78-T81 / M34
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
[ "$#" -le 1 ] || { echo "usage: hot-update-infra-drill-test.sh [all|controls|nats_manifest|caddy_manifest|native_controls|regression_scope]" >&2; exit 2; }
. "$HERE/../lib/supervisor.sh"
DRILL_SUPERVISOR_DIR=
trap '[ -z "$DRILL_SUPERVISOR_DIR" ] || rm -rf "$DRILL_SUPERVISOR_DIR"' EXIT
if [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ]; then
    DRILL_SUPERVISOR_DIR=$(mktemp -d)
    SIM_UPGRADE_SUPERVISOR=$(sim_supervisor_build "$(cd "$HERE/../../.." && pwd)" "$DRILL_SUPERVISOR_DIR")
    export SIM_UPGRADE_SUPERVISOR
fi
[ -x "$SIM_UPGRADE_SUPERVISOR" ] || { echo "infra drill tests: shared supervisor is required" >&2; exit 2; }
"$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B - "$HERE/.." "${1:-all}" <<'PYDRILL'
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(sys.argv[1]).resolve()
MANIFESTS = {
    '85-hot-update-nats': {
        'arms': 'A A1 B D Q L S J F',
        'fixture': 'A=N1 A1=N1 B=N3-live D=N2-live Q=N3-live L=N3-live S=N1 J=N3-live F=N3-live',
        'grows': 'A=0 A1=0 B=2 D=1 Q=2 L=2 S=0 J=2 F=2',
        'worst': 'A=720 A1=720 B=1200 D=300 Q=360 L=360 S=300 J=360 F=540',
        'forgoes': 'A=- A1=- B=- D=- Q=- L=- S=- J=- F=-',
    },
    '86-hot-update-caddy': {
        'arms': 'R B I H C K F U',
        'fixture': 'R=N1 B=N3-live I=N1 H=N2-live C=N1 K=N1 F=N1 U=N1',
        'grows': 'R=0 B=2 I=0 H=1 C=0 K=0 F=0 U=0',
        'worst': 'R=600 B=1200 I=840 H=720 C=300 K=300 F=600 U=720',
        'forgoes': 'R=- B=- I=- H=- C=- K=- F=- U=-',
    },
}
CODES = {'GREEN': 0, 'ASSERT-FAIL': 1, 'SETUP-RED': 2, 'PRODUCT-RED': 3, 'INCOMPLETE': 4}
NATIVE = re.compile(r'^DRILL-VERDICT verdict=(GREEN|ASSERT-FAIL|SETUP-RED|PRODUCT-RED|INCOMPLETE) '
                    r'rc=([0-4]) assert_fail=(\d+) setup_red=(\d+) product_red=(\d+) '
                    r'not_covered=(\d+) nc_gap=(\d+) nc_guard=(\d+) pass=(\d+) -- (.+)$', re.M)


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def command(argv, **kwargs):
    return subprocess.run([os.environ['SIM_UPGRADE_SUPERVISOR'], '--', *map(str, argv)],
                          capture_output=True, text=True, timeout=30, **kwargs)


def manifest(text, expected):
    for name, value in expected.items():
        require(re.findall(r'^# ' + name + r': (.*)$', text, re.M) == [value],
                'wrong/missing/duplicate manifest ' + name)
    require(not re.search(r'^# (fixtures|grow|arms?):.*_', text, re.M), 'illegal arm spelling')


def native(text, rc):
    rows = NATIVE.findall(text)
    # Count markers as well as valid rows: a second malformed verdict must not
    # disappear just because the strict grammar did not match it.
    # origin: hot-update S9 plan §9.3 / T80 / M34
    require(len(rows) == 1 and len(re.findall(r'^DRILL-VERDICT\b', text, re.M)) == 1,
            'missing/duplicate/malformed native verdict')
    verdict, code, *values, detail = rows[0]
    counters = list(map(int, values))
    require(type(rc) is int and rc == int(code) == CODES[verdict], 'translated native rc')
    fail, setup, product, uncovered, gap, guard, passed = counters
    require(uncovered == gap + guard and (verdict != 'GREEN' or passed > 0), 'empty/native counters disagree')
    wanted = next((label for count, label in zip((fail, setup, product, uncovered),
                  ('ASSERT-FAIL', 'SETUP-RED', 'PRODUCT-RED', 'INCOMPLETE')) if count), 'GREEN')
    require(verdict == wanted, 'native counters do not support verdict')
    return verdict, counters


def nats_manifest():
    check_manifest('85-hot-update-nats')


def caddy_manifest():
    check_manifest('86-hot-update-caddy')


def check_manifest(name):
    path = ROOT / 'drills' / (name + '.sh')
    text = path.read_text()
    manifest(text, MANIFESTS[name])
    # Run only the shared metadata reader and static lint. The fixture remains
    # a read-only file, including every shell branch and its unknown-arm guard.
    result = command(['sh', '-c', '. "$1/lib/manifest.sh"; manifest_arms "$2"', 'infra-manifest', ROOT, path])
    require(result.returncode == 0 and result.stdout.strip() == MANIFESTS[name]['arms'], 'real manifest reader disagrees')
    with tempfile.TemporaryDirectory(prefix='infra-manifest-') as directory:
        (Path(directory) / path.name).write_text(text)
        result = command(['sh', ROOT / 'tests/arm-manifest-lint.sh'], env=dict(os.environ, DRILLS=directory))
        require(result.returncode == 0, result.stdout + result.stderr)
    require('--negative-control' not in text, 'invented negative-control entry')
    require(not re.search(r'\bM\d{2}\b', MANIFESTS[name]['arms']), 'mutant included in positive parent')


def native_controls():
    # This is the real assertion implementation, with controlled counters, not
    # an executable drill or a fake product outcome. Every native code survives.
    for counter, verdict in (('_AS_PASS', 'GREEN'), ('_AS_FAIL', 'ASSERT-FAIL'), ('_AS_SETUP', 'SETUP-RED'),
                             ('_AS_PRODUCT_RED', 'PRODUCT-RED'), ('_AS_NC', 'INCOMPLETE')):
        body = ('. "$1/lib/assert.sh"\nlog() { :; }; ok() { :; }; err() { :; }\n'
                'INSTANCE=drill-infra-hermetic\ndrill_begin infra-hermetic\n_AS_PASS=1\n' + counter + '=1\n')
        if counter == '_AS_NC':
            body += '_AS_NC_GAP=1\n'
        result = command(['sh', '-c', body + 'drill_end\n', 'infra-native', ROOT])
        require(native(result.stderr, result.returncode)[0] == verdict, 'actual native assertion code changed')
        if verdict != 'GREEN':
            result = command(['sh', '-c', body.replace('_AS_PASS=1', '_AS_PASS=0') + 'drill_end\n', 'infra-native', ROOT])
            require(native(result.stderr, result.returncode)[0] == verdict, 'native early-red with zero passes was translated')
    # A manual mutant is assertion failure; no translation to historical loss.
    controls()


def table(text):
    rows = [line.split('\t') for line in text.splitlines() if line and not line.startswith('#')]
    require(all(len(row) == 6 for row in rows), 'expected-verdict table width')
    require(len({row[0] for row in rows}) == len(rows), 'duplicate expected-verdict row')
    return {row[0]: row for row in rows}


def regression_scope():
    rows = table((ROOT / 'expected-verdicts.tsv').read_text())
    for name in ('32-install-lifecycle', '73-proxy-cluster-ha'):
        require(rows[name][:4] == [name, 'GREEN', '0', '-'], 'regression native expectation changed: ' + name)
    row = rows['98-stuck-redial-recovery']
    require(row[1:4] == ['INCOMPLETE', '1', '-'] and '#72' in row[4], '98 WSS gap relabelled as success')
    for arm, verdict in (('X', 'GREEN'), ('Y', 'PRODUCT-RED'), ('P', 'PRODUCT-RED'), ('F', 'PRODUCT-RED'),
                         ('BR', 'PRODUCT-RED'), ('NI', 'GREEN'), ('CI', 'GREEN')):
        name = '39-hot-update-legacy-n1.' + arm
        require(rows[name][1:4] == [verdict, '0', '-'], 'legacy native expectation changed: ' + arm)
    install = (ROOT / 'drills/32-install-lifecycle.sh').read_text()
    for token in ('--dry-run', '--uninstall', 'place_binary', 'manual', '%u', '%g', 'sha256sum',
                  '/usr/local/libexec', '/var/lib/tether-infra', '/run/tether-infra',
                  'tether-infra-helper', 'tether-infra-launch', 'nats-server', 'caddy'):
        require(token in install, '32 lost original or expanded installation scope: ' + token)
    proxy = (ROOT / 'drills/73-proxy-cluster-ha.sh').read_text()
    require('_ss_leg_up' in proxy and 'curl' in proxy, '73 lost actual SS/curl recovery')
    stuck = (ROOT / 'drills/98-stuck-redial-recovery.sh').read_text()
    require('not_covered' in stuck and '#72' in stuck and 'nats://' in stuck, '98 lost original fault and coverage boundary')


def controls():
    for expected in MANIFESTS.values():
        good = '\n'.join('# ' + key + ': ' + value for key, value in expected.items()) + '\n'
        manifest(good, expected)
        for key in expected:
            line = '# ' + key + ': ' + expected[key] + '\n'
            for bad in (good.replace(line, ''), good + line, good.replace(line, line.replace('A', 'X', 1))):
                if bad == good:
                    bad = good.replace(line, '# ' + key + ': bad\n')
                try:
                    manifest(bad, expected)
                except ValueError:
                    pass
                else:
                    raise ValueError('manifest mutation survived: ' + key)
    green = 'DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- control\n'
    native(green, 0)
    for text, rc in ((green, 1), (green + green, 0), (green + 'DRILL-VERDICT malformed\n', 0),
                     (green.replace('pass=1', 'pass=0'), 0),
                     (green.replace('assert_fail=0', 'assert_fail=1'), 0),
                     (green.replace('not_covered=0', 'not_covered=1'), 0)):
        try:
            native(text, rc)
        except ValueError:
            pass
        else:
            raise ValueError('native verdict mutation survived')


def main():
    selection = sys.argv[2]
    cases = dict(nats_manifest=nats_manifest, caddy_manifest=caddy_manifest,
                 native_controls=native_controls, regression_scope=regression_scope, controls=controls)
    require(selection == 'all' or selection in cases, 'unknown infra drill test selection')
    selected = list(cases)[:-1] if selection == 'all' else [selection]
    missing = []
    if any(case in selected for case in ('nats_manifest', 'caddy_manifest', 'regression_scope')):
        missing = [name for name in MANIFESTS if not (ROOT / 'drills' / (name + '.sh')).is_file()]
    if missing:
        print('NOT_RUN pending P33: ' + ', '.join(missing), file=sys.stderr)
        return 2
    for name in selected:
        cases[name]()
        print('infra drill ' + name + ': PASS')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, KeyError, ValueError, subprocess.SubprocessError) as error:
        print('infra drill tests: ' + str(error), file=sys.stderr)
        sys.exit(1)
PYDRILL
