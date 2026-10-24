"""Hermetic compatibility actors, obligation tables and deployment sandbox."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

SIM = Path(__file__).resolve().parents[1]
REPO = SIM.parents[1]
DRILL_NAME = '39-hot-update-legacy-n1'
DRILL = SIM / 'drills' / (DRILL_NAME + '.sh')
WIRE_ARGV = REPO / 'test/hotupdate/testdata/upgrade_wire_argv.json'
EXPECTED = SIM / 'expected-verdicts.tsv'
# Owned by other r9d sections (historical/legacy and infra refusal arms).
NOT_COMPAT = ('X', 'Y', 'P', 'F', 'BR', 'NI', 'CI')
KINDS = ('wire', 'refusal', 'non_hot', 'hot', 'decoder', 'config')
VERDICTS = ('GREEN', 'ASSERT-FAIL', 'SETUP-RED', 'PRODUCT-RED', 'INCOMPLETE')
VERDICT = re.compile(r'^DRILL-VERDICT verdict=(\S+) rc=(\d+) assert_fail=(\d+) setup_red=(\d+) '
                     r'product_red=(\d+) not_covered=(\d+) nc_gap=(\d+) nc_guard=(\d+) pass=(\d+) -- ', re.M)
OLD_SHA, NEW_SHA = 'a' * 64, 'b' * 64
NONCE = '0123456789abcdef0123456789abcdef'
VALUES = dict(HOME_CELL='/isolated/wire-home', NATS_URL='nats://127.0.0.1:4222', NID='agt1', PID='4242', NONCE=NONCE)

# Independent pin of the obligations each arm's judge must certify. Kept
# separate from the drill on purpose: a check removed from PYCOMPAT leaves its
# negative below GREEN, and that is a red control.
REQUIRED = {
    'LB-new': ['legacy_busy', 'all_entry_policy', 'running_unchanged', 'original_business'],
    'LB-old': ['legacy_busy', 'old_ctl_rc64', 'all_entry_policy', 'running_unchanged', 'original_business'],
    'LI-defer': ['quiescence_unknown', 'running_unchanged', 'original_business'],
    'LI-interrupting': ['all_consumers_capable', 'unique_issuer', 'dispatch_once', 'outcome_unknown', 'non_hot'],
    'M-route': ['mixed_consumer', 'route_changed', 'no_dispatch', 'original_business'],
    'CC-idle': ['complete_idle_census', 'fence_held', 'three_receipts'],
    'CC-late-create': ['late_create_fenced', 'no_untracked_admission', 'three_receipts'],
    'CL-reject': ['required_reader_missing', 'preactivation_refusal', 'original_business'],
    'ABI-adjacent': ['nonempty_forward_decode', 'nonempty_reverse_decode', 'three_receipts'],
    'ABI-too-far': ['two_epoch_gap', 'preactivation_refusal', 'original_business'],
    'ABI-unknown': ['unknown_reader', 'preactivation_refusal', 'original_business'],
    'GO-hash': ['distinct_toolchains', 'nonempty_checkpoint', 'continued_digest_equal'],
}
# Independent pin of each arm's CLI statuses (S8 plan §9.3): a refusal whose
# expected status is silently rewritten to success would make the drill and
# this driver agree on an acceptance, so CASES39 must equal this table.
EXPECTED_CLI = {'LB-new': [64], 'LB-old': [64], 'LI-defer': [75], 'LI-interrupting': [74], 'M-route': [75],
                'CC-idle': [0], 'CC-late-create': [0, 75], 'CL-reject': [64], 'ABI-adjacent': [0],
                'ABI-too-far': [64], 'ABI-unknown': [64], 'GO-hash': [0], 'CFG-nats-old': [0], 'CFG-nats-new': [0],
                'CFG-caddy-old': [0], 'CFG-caddy-new': [0],
                **{'Q%sB%s-%s' % (q, b, verb): [0, 0] if verb in ('expose', 'transfer') else [0]
                   for q in ('00', '01', '10', '11') for b in '01' for verb in ('exec', 'run', 'kill', 'expose', 'transfer')}}
WIRE_REQUIRED = {'exec': ['exact_nonce', 'one_execution'], 'run': ['real_pty', 'input_output', 'resize', 'unique_terminal'],
                 'kill': ['original_pid', 'signal15', 'unique_terminal', 'old_sender_when_applicable'],
                 'expose': ['public_port_nonce', 'cleanup'], 'transfer': ['tier_b_8mib', 'push_pull_hash', 'unique_terminals']}

SUPPLIER = r'''import json, os, pathlib, sys, time
fake = pathlib.Path(os.environ['FAKE_DIR'])
op, args = sys.argv[1], sys.argv[2:]
# The documented port argv: <op> --arm A --evidence ROOT --supervisor S.
if len(args) != 6 or args[0::2] != ['--arm', '--evidence', '--supervisor']:
    sys.exit(96)
arm, root = args[1], pathlib.Path(args[3])
if args[5] != os.environ.get('SIM_UPGRADE_SUPERVISOR'):
    sys.exit(95)
scenario = json.loads((fake / 'scenario.json').read_text())
with (fake / 'calls').open('a') as log:
    log.write('supplier ' + op + ' ' + arm + '\n')
if op == 'prepare':
    (root / 'plan.json').write_text(json.dumps(scenario['plan']))
    sys.exit(scenario['rc'].get('prepare', 0))
if op == 'await-fence':
    # Observe the live upgrade at its fence: the original command must be up.
    if scenario['fence'] != 'observe':
        sys.exit(1)
    until = time.monotonic() + 10
    while not (fake / 'ready').exists():
        if time.monotonic() > until:
            sys.exit(1)
        time.sleep(0.01)
    (fake / 'fence').touch()
    sys.exit(0)
if op == 'observe':
    (root / 'executions.json').write_text(json.dumps(scenario['executions']))
    sys.exit(scenario['rc'].get('observe', 0))
if op == 'judge':
    # The judge reads the driver's retained command statuses; it does not
    # receive them from the scenario, so the driver's own wanted-rc gate is
    # the only thing that can turn a wrong CLI status red.
    statuses, slot = [], 0
    while (root / ('command-%d.rc' % slot)).exists():
        statuses.append(int((root / ('command-%d.rc' % slot)).read_text()))
        slot += 1
    result = dict(version=1, arm=arm, kind=scenario['kind'], native_rc=scenario['judge_rc'],
                  command_rcs=statuses, signature='hermetic-' + arm, checks=scenario['checks'])
    (root / 'result.json').write_text(json.dumps(result))
    sys.exit(scenario['judge_rc'])
if op == 'stop':
    (root / 'cleanup.json').write_text(json.dumps(scenario['cleanup']))
    sys.exit(scenario['rc'].get('stop', 0))
sys.exit(64)
'''

# A finite product actor. Slot modes model only scheduling around the fence;
# the status comes from the scenario. It never forks a survivor.
ACTOR = r'''#!/bin/sh
[ "$1" = --slot ] || exit 96
slot=$2; shift 2
{ printf 'command %s' "$slot"; for a in "$@"; do printf '\037%s' "$a"; done; printf '\n'; } >> "$FAKE_DIR/calls"
mode=$(cat "$FAKE_DIR/mode.$slot" 2>/dev/null) || mode=plain
case "$mode" in
    hold)
        # One process for the whole live window: a terminate from the driver
        # leaves no orphaned sleep for the shared owner to report.
        exec python3 -I -B -c 'import pathlib, sys, time
fake = pathlib.Path(sys.argv[1]); (fake / "ready").touch(); until = time.monotonic() + 10
while not (fake / "done").exists():
    if time.monotonic() > until: sys.exit(97)
    time.sleep(0.01)
sys.exit(int((fake / ("rc." + sys.argv[2])).read_text()))' "$FAKE_DIR" "$slot" ;;
    early) : > "$FAKE_DIR/ready" ;;
    late) [ -e "$FAKE_DIR/fence" ] || exit 9; : > "$FAKE_DIR/done" ;;
esac
exit "$(cat "$FAKE_DIR/rc.$slot")"
'''

# Double of the P26 live-fixture interface consumed by PYCOMPAT for
# ABI-adjacent. It records lifecycle order and answers with the scenario's
# judge status; it is NOT the full-profile oracle and certifies nothing.
FIXTURE = r'''"""Hermetic double of the P26 upgrade_tx_fixture interface (r9d compat controls)."""
import json, os, sys
from pathlib import Path
FAKE = Path(os.environ['FAKE_DIR'])
SCENARIO = json.loads((FAKE / 'scenario.json').read_text())['fixture']


def _record(event):
    with (FAKE / 'calls').open('a') as log:
        log.write('fixture ' + event + '\n')


def manifest_read(path):
    value = json.loads(Path(path).read_text())
    _record('manifest_read ' + value['arm'])
    return value


def start(root, manifest, lifeline_fd):
    os.fstat(lifeline_fd)
    Path(root).mkdir(mode=0o700, parents=True, exist_ok=False)
    _record('start')


class collect:
    @staticmethod
    def sample(root, case_id, nonce):
        if not Path(root).is_dir() or len(nonce) != 32:
            raise ValueError('sample_without_started_fixture')
        _record('sample ' + case_id)


def drain(root, manifest):
    _record('drain')


def stop(root, manifest):
    _record('stop')
    if SCENARIO['stop'] != 'ok':
        raise RuntimeError('fixture_cleanup_not_proven')


if __name__ == '__main__':
    if sys.argv[1:2] != ['judge'] or sys.argv[2::2] != ['--manifest', '--evidence']:
        sys.exit(96)
    _record('judge')
    sys.exit(SCENARIO['judge_rc'])
'''


def require(ok, reason):
    if not ok:
        raise AssertionError(reason)


def cases():
    """CASES39 rows from the REAL drill, reconciled with its `# arms:` header."""
    source = DRILL.read_text()
    require(source.count("cat <<'CASES39'\n") == 1, 'CASES39 table missing or duplicated')
    body = source.split("cat <<'CASES39'\n", 1)[1].split('\nCASES39\n', 1)[0]
    rows = [line.split('\t') for line in body.splitlines()]
    require(rows and all(len(row) == 7 for row in rows), 'CASES39 row width changed')
    header = re.findall(r'^# arms: (.*)$', source, re.M)
    require(len(header) == 1 and header[0].split() == [row[0] for row in rows],
            'CASES39 rows drifted from the drill # arms: header')
    table = {}
    for arm, kind, native, cli, fixture, grows, worst in rows:
        require(arm not in table, 'duplicate CASES39 arm ' + arm)
        table[arm] = dict(arm=arm, kind=kind, native=int(native), cli=[int(x) for x in cli.split(',')],
                          fixture=fixture, grows=int(grows), worst=int(worst))
    return table


CASES = cases()
COMPAT = [arm for arm in CASES if arm not in NOT_COMPAT]
require(all(arm in CASES for arm in NOT_COMPAT), 'historical/infra arm vanished from CASES39')
require(all(CASES[arm]['kind'] in KINDS for arm in COMPAT), 'unknown compatibility kind')


def expected_verdict(arm):
    rows = [line.split('\t') for line in EXPECTED.read_text().splitlines() if line and not line.startswith('#')]
    match = [row for row in rows if row[0] == DRILL_NAME + '.' + arm]
    require(len(match) == 1, 'expected-verdicts row missing or duplicated for ' + arm)
    verdict = match[0][1]
    require(verdict in VERDICTS and VERDICTS.index(verdict) == CASES[arm]['native'],
            'expected-verdicts disagrees with CASES39 native for ' + arm)
    return verdict


def required_checks(arm):
    case = CASES[arm]
    if case['kind'] == 'wire':
        return list(WIRE_REQUIRED[arm.split('-')[1]])
    if case['kind'] == 'config':
        _, component, selected = arm.split('-')
        return ['same_rendered_config', 'actual_' + component + '_parser', 'selected_' + selected + '_binary']
    return list(REQUIRED[arm])


def quadrant(path):
    return OLD_SHA if '/old/' in path else NEW_SHA


def flipped(path):
    return path.replace('/old/', '/@/').replace('/new/', '/old/').replace('/@/', '/new/')


def bind(arg, values=VALUES):
    for key in ('HOME_CELL', 'NATS_URL', 'NID', 'PID', 'NONCE'):
        arg = arg.replace(key, values[key])
    return arg


def templates():
    rows = json.loads(WIRE_ARGV.read_text())
    require(len(rows) == len({(r['cell'], r['verb']) for r in rows}), 'duplicate frozen wire template')
    return rows


def template(cell, verb):
    match = [row for row in templates() if row['cell'] == cell and row['verb'] == verb]
    require(len(match) == 1, 'frozen wire template missing for ' + cell + '-' + verb)
    return match[0]


def wire_manifest():
    """A P22-shaped manifest: the frozen rows plus per-cell companions."""
    rows, companions = [], {}
    common = ['--home', VALUES['HOME_CELL'], '--nats-url', VALUES['NATS_URL']]
    for row in templates():
        rows.append(dict(row, ctl_sha=quadrant(row['ctl_path']), agent_sha=quadrant(row['agent_path']),
                         broker_sha=quadrant(row['broker_path']), source_commit='c' * 40))
        ctl = row['ctl_path']
        companions[row['cell']] = dict(
            expose_cleanup=[ctl, 'expose', 'rm', *common, '--name', 'wire-' + NONCE, VALUES['NID']],
            transfer_pull=[ctl, 'pull', *common, '--timeout', '120s', VALUES['NID'] + ':/tmp/s8/' + NONCE + '/dest',
                           '/tmp/s8/' + NONCE + '/back'])
    return dict(schema_version=1, rows=rows, companions=companions, execution='NOT_RUN')


def execution_row(path):
    sha = quadrant(path)
    return dict(schema_version=1, pid=4100, start_ticks=9100, boot_id='hermetic-boot', observed_at='2026-10-13T00:00:00Z',
                running_sha256=sha, copied_sha256=sha, copied_path=path, proc_exe=path, argv=[path, 'serve'],
                artifact=dict(binary_sha256=sha, commit='c' * 40, tree_digest='d' * 64, archive_sha256='e' * 64,
                              toolchain='go1.26.8', build_flags=['-trimpath'], dirty_diff_sha256='f' * 64))


def products(arm):
    """Product argv per command slot: frozen wire templates, real parser flags, CLI verbs."""
    case = CASES[arm]
    if case['kind'] == 'wire':
        cell, verb = arm.split('-')
        primary = [bind(arg) for arg in template(cell, verb)['argv']]
        if verb in ('expose', 'transfer'):
            companion = wire_manifest()['companions'][cell]['expose_cleanup' if verb == 'expose' else 'transfer_pull']
            return [primary, companion]
        return [primary]
    if case['kind'] == 'config':
        _, component, selected = arm.split('-')
        if component == 'nats':
            return [['/opt/s8/' + selected + '/nats-server', '-t', '-c', '/etc/tether/nats.d/nats.conf']]
        return [['/opt/s8/' + selected + '/caddy', 'validate', '--config', '/etc/caddy/Caddyfile', '--adapter', 'caddyfile']]
    if case['kind'] == 'decoder':
        return [['/opt/s8/new/tether-checkpoint-reader', 'decode', '--checkpoint', '/isolated/checkpoint.bin']]
    ctl = '/opt/s8/old/tether' if arm == 'LB-old' else '/opt/s8/new/tether'
    upgrade = [ctl, 'node', 'upgrade', 'agt1', '--url', 'https://artifact.invalid/next', '--sha256', NEW_SHA]
    if arm == 'LI-interrupting':
        upgrade.append('--interrupting')
    if arm == 'CC-late-create':
        return [upgrade, ['/opt/s8/new/tether', 'exec', '--home', '/isolated/late', 'agt1', '--', '/bin/true']]
    return [upgrade]


def roles(arm):
    case = CASES[arm]
    if case['kind'] == 'config':
        return {'parser': products(arm)[0][0]}
    if case['kind'] == 'decoder':
        return {'exporter': '/opt/s8/old/tether', 'reader': '/opt/s8/new/tether-checkpoint-reader'}
    if case['kind'] == 'wire':
        row = template(*arm.split('-'))
        return {role: row[role + '_path'] for role in ('ctl', 'agent', 'broker')}
    return {'ctl': '/opt/s8/new/tether', 'agent': '/opt/s8/old/tether', 'broker': '/opt/s8/old/tether'}


class Outcome:
    def __init__(self, sandbox, result):
        self.sandbox, self.rc, self.stderr = sandbox, result.returncode, result.stdout + result.stderr
        self.rows = VERDICT.findall(self.stderr)
        row = self.rows[-1] if self.rows else None
        self.verdict = row[0] if row else None
        self.counts = dict(zip(('rc', 'assert_fail', 'setup_red', 'product_red', 'not_covered', 'nc_gap',
                                'nc_guard', 'pass'), map(int, row[1:]))) if row else {}
        evidence = sorted(sandbox.evidence.glob('hot-update-compat.*'))
        self.receipt = None
        if len(evidence) == 1 and (evidence[0] / 'receipt.json').is_file():
            self.receipt = json.loads((evidence[0] / 'receipt.json').read_text())
        self.evidence = evidence

    def describe(self):
        return 'rc=%s verdict=%s counts=%s receipt=%s\noutput tail:\n%s\ncalls:\n%s' % (
            self.rc, self.verdict, self.counts, self.receipt, '\n'.join(self.stderr.splitlines()[-20:]),
            '\n'.join(self.sandbox.calls()))


class Sandbox:
    """A private HERE carrying the REAL drill tree and a recording boundary."""

    def __init__(self, arm):
        self.arm, self.case = arm, CASES[arm]
        self.temp = tempfile.TemporaryDirectory(prefix='compat-arm-' + arm + '-')
        self.root = Path(self.temp.name)
        self.here = self.root / 'repo/test/simcluster'
        self.here.mkdir(parents=True)
        (self.here / 'lib').symlink_to(SIM / 'lib')
        (self.here / 'drills/lib').mkdir(parents=True)
        for entry in (SIM / 'drills').iterdir():
            if entry.name != 'lib':
                (self.here / 'drills' / entry.name).symlink_to(entry)
        for entry in (SIM / 'drills/lib').iterdir():
            if entry.name == '__pycache__':
                continue
            if entry.name == 'upgrade_tx_fixture.py' and arm == 'ABI-adjacent':
                self.write(self.here / 'drills/lib' / entry.name, FIXTURE)
                continue
            (self.here / 'drills/lib' / entry.name).symlink_to(entry)
        testdata = self.root / 'repo/test/hotupdate/testdata'
        testdata.mkdir(parents=True)
        (testdata / WIRE_ARGV.name).symlink_to(WIRE_ARGV)
        self.write(self.here / 'simcluster', '#!/bin/sh\necho "hermetic SIM must never execute" >&2\nexit 98\n', True)
        self.bin = self.root / 'bin'
        for name in ('docker', 'ssh', 'sudo', 'systemctl', 'journalctl', 'tether', 'runuser', 'nsenter', 'iptables'):
            self.write(self.bin / name, '#!/bin/sh\nprintf "%s\\n" "$0 $*" >> "$FAKE_DIR/poisoned"\nexit 99\n', True)
        self.fake = self.root / 'fake'
        self.write(self.fake / 'supplier.py', SUPPLIER)
        self.write(self.fake / 'supplier', '#!/bin/sh\nexec python3 -I -B "$FAKE_DIR/supplier.py" "$@"\n', True)
        self.write(self.fake / 'actor', ACTOR, True)
        self.write(self.fake / 'wire.json', json.dumps(wire_manifest()))
        self.write(self.fake / 'fixture-manifest.json', json.dumps(dict(arm=arm, case_id=arm, version=1)))
        self.evidence = self.root / 'evidence'
        self.evidence.mkdir()

    @staticmethod
    def write(path, text, executable=False):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        if executable:
            path.chmod(0o700)

    def close(self):
        self.temp.cleanup()

    def scenario(self):
        """The positive scenario: every obligation of this arm satisfied."""
        arm, case = self.arm, self.case
        commands = []
        for slot, product in enumerate(products(arm)):
            commands.append(dict(argv=[str(self.fake / 'actor'), '--slot', str(slot), *product], product_argv=product,
                                 cwd=str(self.fake), timeout_s=min(30, case['worst'])))
        plan = dict(version=1, arm=arm, commands=commands)
        if case['kind'] == 'wire':
            plan.update(values=dict(VALUES), wire_manifest=str(self.fake / 'wire.json'))
        if arm == 'ABI-adjacent':
            plan['fixture_manifest'] = str(self.fake / 'fixture-manifest.json')
        modes = {}
        if arm == 'CC-late-create':
            modes = {'0': 'hold', '1': 'late'}
        return dict(plan=plan, kind=case['kind'], executions={role: execution_row(path) for role, path in roles(arm).items()},
                    checks={key: True for key in required_checks(arm)}, judge_rc=case['native'],
                    cli={str(slot): rc for slot, rc in enumerate(case['cli'])}, modes=modes, rc={}, fence='observe',
                    cleanup=dict(empty=True, remaining=[]), fixture=dict(judge_rc=0, stop='ok'))

    def calls(self):
        path = self.fake / 'calls'
        return path.read_text().splitlines() if path.exists() else []

    def run(self, scenario):
        for name in ('calls', 'poisoned', 'ready', 'fence', 'done'):
            (self.fake / name).unlink(missing_ok=True)
        for old in list(self.fake.glob('rc.*')) + list(self.fake.glob('mode.*')):
            old.unlink()
        for old in self.evidence.iterdir():
            shutil.rmtree(old)
        for slot, rc in scenario['cli'].items():
            self.write(self.fake / ('rc.' + slot), str(rc) + '\n')
        for slot, mode in scenario['modes'].items():
            self.write(self.fake / ('mode.' + slot), mode + '\n')
        self.write(self.fake / 'scenario.json', json.dumps(scenario))
        env = dict(PATH=str(self.bin) + os.pathsep + os.environ.get('PATH', os.defpath), HOME=str(self.root / 'home'),
                   LC_ALL='C', HERE=str(self.here), ARM=self.arm, SIM_DRILL_NAME=DRILL_NAME,
                   INSTANCE='drill-' + DRILL_NAME, SIM_EVIDENCE_DIR=str(self.evidence), SIM=str(self.here / 'simcluster'),
                   SIM_UPGRADE_SUPERVISOR=os.environ['SIM_UPGRADE_SUPERVISOR'],
                   SIM_UPGRADE_COMPAT_SUPPLIER=str(self.fake / 'supplier'), FAKE_DIR=str(self.fake))
        result = subprocess.run(['/bin/sh', str(self.here / 'drills' / (DRILL_NAME + '.sh'))], cwd=self.root, env=env,
                                capture_output=True, text=True, timeout=120)
        return Outcome(self, result)


