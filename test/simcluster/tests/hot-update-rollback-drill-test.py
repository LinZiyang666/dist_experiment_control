#!/usr/bin/env python3
"""Two-sided arm controls for drill 38 (r9d-nonvacuity D-11 sections).

origin: hot-update S8 plan §9.2 / §10 / §11.2 P27; r9d D-11.

Every arm runs the REAL drill file (its own `case "${ARM:?}"` label, the real
upgrade38_run heredoc program PY38, the real upgrade38_native mapping and the
real lib/assert.sh verdict) against the arm's REAL row in
drills/lib/upgrade_tx_arms.tsv and the REAL upgrade_fault_cases.tsv, inside a
private sandbox under the shared FIX-ORPHAN supervisor. Only the deployment
boundary is a double (tests/hot_update_rollback_*_double.py): the P26 fixture's
live start/stop/drain/sample/judge/companion, the supervisor's command execution,
the fault injector's environment effect, the mutation build supplier and the
interpreter clock that the family-A calibrations sleep on. docker, ssh, sudo,
systemctl, journalctl and tether are poisoned on PATH. Nothing here is a cut,
rollback, kill or deployment receipt.

Each arm must (1) reach its expected native verdict from expected-verdicts.tsv
on good evidence, with the trace proving the runner walked exactly the arm's
declared operations, and (2) be driven off it — to one SPECIFIC native status
with its own reason — by deliberately bad input, at least one aimed at the
arm's own obligation (its last command's CLI rc, its proof set, R checkpoint
and reverse receipts, W injector/hit, A calibration interval, H reverse step,
M causal kill receipt, I judge binding). An oracle that cannot leave its expected
verdict is the permanently-true family r9d exists for; one that cannot reach it
is a permanently-red arm nobody can sign.

Usage: python3 -I -B tests/hot-update-rollback-drill-test.py --arm <ARM>|all [--results DIR] [--jobs N]
"""
import argparse
import csv
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

TESTS = Path(__file__).resolve().parent
SIM = TESTS.parent
REPO = SIM.parents[1]
NAME = '38-hot-update-rollback-compat'
DRILL = SIM / 'drills' / (NAME + '.sh')
ARMS_TSV = SIM / 'drills/lib/upgrade_tx_arms.tsv'
REAL_LIB = SIM / 'drills/lib'
EXPECTED = SIM / 'expected-verdicts.tsv'
SANDBOX_LINKS = ('lib/log.sh', 'lib/assert.sh', 'lib/supervisor.sh', 'drills/' + NAME + '.sh',
                 'drills/lib/upgrade_tx_arms.tsv')
DOUBLES = {'drills/lib/upgrade_tx_fixture.py': TESTS / 'hot_update_rollback_fixture_double.py',
           'drills/lib/upgrade_tx_faults.py': TESTS / 'hot_update_rollback_faults_double.py'}
CLOCK = TESTS / 'hot_update_rollback_clock_double.py'
POISONED = ('docker', 'ssh', 'sudo', 'systemctl', 'journalctl', 'tether')
CODES = {'GREEN': 0, 'ASSERT-FAIL': 1, 'SETUP-RED': 2, 'PRODUCT-RED': 3, 'INCOMPLETE': 4}
# The runner's own receipt vocabulary (PY38 VERDICTS), distinct from assert.sh's.
RUNNER_VERDICTS = {0: 'GREEN', 1: 'ASSERT-RED', 2: 'SETUP-RED', 3: 'PRODUCT-RED', 4: 'INCOMPLETE'}
VERDICT = re.compile(r'^DRILL-VERDICT verdict=(\S+) rc=(\d+) assert_fail=(\d+) setup_red=(\d+) '
                     r'product_red=(\d+) not_covered=(\d+) nc_gap=(\d+) nc_guard=(\d+) pass=(\d+) -- ', re.M)
ORIGIN = dict(pid=4242, start_ticks=777)
SUCCESSOR = dict(pid=4242, start_ticks=777, instance_id='successor-runtime')

_spec = importlib.util.spec_from_file_location('upgrade_tx_collect', REAL_LIB / 'upgrade_tx_collect.py')
collect = sys.modules.get(_spec.name)
if collect is None:
    collect = importlib.util.module_from_spec(_spec)
    sys.modules[_spec.name] = collect
    _spec.loader.exec_module(collect)


def table():
    lines = [line for line in ARMS_TSV.read_text().splitlines() if not line.startswith('#')]
    rows = list(csv.DictReader(io.StringIO('\n'.join(lines)), delimiter='\t'))
    return {row['arm']: row for row in rows}


def header_arms():
    values = re.findall(r'^# arms: (.*)$', DRILL.read_text(), re.M)
    return values[0].split() if len(values) == 1 else []


def expected_verdicts():
    result = {}
    for line in EXPECTED.read_text().splitlines():
        cells = line.split('\t')
        if line.startswith('#') or not cells[0].startswith(NAME + '.'):
            continue
        result[cells[0][len(NAME) + 1:]] = cells[1]
    return result


def program_sha():
    # The exact bytes the drill's heredoc feeds "$S8_PYTHON" on stdin.
    body = DRILL.read_text().split("<<'PY38'\n", 1)[1].split('\nPY38\n', 1)[0]
    return hashlib.sha256((body + '\n').encode()).hexdigest()


def operations(row):
    return row['operations'].split(',')


def base_scenario(row):
    ops = operations(row)
    command_rc = {op: 0 for op in ops}
    command_rc[ops[-1]] = int(row['cli_rc'])
    return dict(family=row['family'], command_rc=command_rc, judge_rc=int(row['expected_native']),
                judge_proofs=row['proofs'].split(','))


def build_inputs(root, row, shared):
    spec = dict(argv=[shared, '--', '/usr/bin/true'], cwd=str(root), timeout_s=5)
    family = row['family']
    case_id = row['arm'] if row['case_id'] == 'none' else row['case_id']
    actors = [dict(id=kind + str(n), family=kind, resource_ids=[kind + str(n)], command=dict(spec))
              for kind, count in collect.PROFILE.items() for n in range(count)]
    faults = []
    if family == 'W':
        faults = [dict(id=case_id, mechanism='syscall_delay', syscall='write', errno=5, target_resource='exec0',
                       arm=dict(spec), release=dict(spec))]
    old = dict(commit='a' * 40, binary_sha256='a' * 64, toolchain='go1.26.8', build_flags=['-trimpath'],
               dirty_diff_sha256='d' * 64, source_archive=str(root / 'source-a.tar'), archive_sha256='1' * 64)
    new = dict(old, commit='c' * 40, binary_sha256='b' * 64, source_archive=str(root / 'source-b.tar'),
               archive_sha256='2' * 64)
    pair = dict(schema_version=1, a=old, b=new, implementation_digests=['impl-a', 'impl-b'],
                compiled_implementation_proofs=[dict(binary_sha256='a' * 64), dict(binary_sha256='b' * 64)])
    manifest = dict(version=1, source_commit=new['commit'], artifact_sha=new['binary_sha256'], run_id='1' * 32,
                    arm=row['arm'], case_id=case_id, role=row['role'], topology=row['topology'], actors=actors,
                    ports={port: dict(spec) for port in ('observe', 'admissions', 'drain')},
                    domains=sorted(collect.DOMAINS), faults=faults, deadline_s=2 * int(row['worst']),
                    expect=dict(phase=row['expected_phase'], outcome=row['expected_outcome'], native_rc=int(row['cli_rc']),
                                completion=row['expected_phase'] in ('completed', 'rolled_back'),
                                hit_required=family == 'W', a6=family == 'A'),
                    transfer_budget=dict(rate_bytes_s=1024, cut_budget_s=1100, drain_budget_s=10, margin_s=10,
                                         size_bytes=2000000))
    document = dict(version=1, arm=row['arm'], fixture=str(root / 'fixture.json'), pair=str(root / 'pair.json'),
                    operations=[dict(name=op, command=None if op in ('inject', 'release') else dict(spec))
                                for op in operations(row)],
                    mutation_patch_digest='none')
    mutation = None
    if family == 'M':
        recipe = json.loads(row['mutation'])
        patch = hashlib.sha256(json.dumps(recipe, sort_keys=True).encode()).hexdigest()
        mutation = dict(version=1, arm=row['arm'], recipe=recipe, artifact=new, patch_digest=patch, binary_sha='e' * 64)
        document.update(mutation=str(root / 'mutation.json'), mutation_patch_digest=patch)
        if recipe['build'] != 'python':
            # A compiled mutant runs its own rebuilt bytes from B's source.
            manifest['artifact_sha'] = mutation['binary_sha']
    return manifest, pair, document, mutation


class Run:
    def __init__(self, rc, stdout, stderr, trace, receipt_file, poison):
        self.rc, self.stdout, self.stderr, self.trace, self.receipt_file, self.poison = (
            rc, stdout, stderr, trace, receipt_file, poison)
        self.verdicts = VERDICT.findall(stderr)
        lines = [line for line in stdout.splitlines() if line.startswith('{')]
        self.receipt = json.loads(lines[-1]) if len(lines) == 1 else None
        self.receipt_lines = len(lines)

    def events(self, *kinds):
        return [row for row in self.trace if row['event'] in kinds]

    def walked(self):
        # Operations the runner actually dispatched, in order: supervised
        # commands plus the injector's arm/release.
        return [row['operation'] if row['event'] == 'run' else row['event']
                for row in self.events('run', 'inject', 'release')]


def run_arm(row, scenario_edit=None, manifest_edit=None, document_edit=None):
    shared = os.environ['SIM_UPGRADE_SUPERVISOR']
    tmp = Path(tempfile.mkdtemp(prefix='r9d38-' + row['arm'] + '-'))
    try:
        sim = tmp / 'sim'
        for rel in SANDBOX_LINKS:
            (sim / rel).parent.mkdir(parents=True, exist_ok=True)
            (sim / rel).symlink_to(SIM / rel)
        for rel, source in DOUBLES.items():
            shutil.copyfile(source, sim / rel)
        poison = tmp / 'poison'
        poison.mkdir()
        for name in POISONED:
            (poison / name).write_text('#!/bin/sh\nprintf "%s %s\\n" "$0" "$*" >> "$R9D38_POISON"\nexit 97\n')
            (poison / name).chmod(0o755)
        bin_dir = tmp / 'bin'
        bin_dir.mkdir()
        wrapper = bin_dir / 'python-clock'
        # The drill invokes "$S8_PYTHON" -I -B - <args>; anything else is refused.
        wrapper.write_text('#!/bin/sh\n[ "$#" -ge 3 ] && [ "$1" = -I ] && [ "$2" = -B ] && [ "$3" = - ] || exit 96\n'
                           'shift 3\nexec /usr/bin/python3 -I -B "$R9D38_CLOCK" "$@"\n')
        wrapper.chmod(0o755)
        manifest, pair, document, mutation = build_inputs(tmp, row, shared)
        scenario = base_scenario(row)
        for edit, value in ((manifest_edit, manifest), (document_edit, document), (scenario_edit, scenario)):
            if edit:
                edit(value)
        (tmp / 'fixture.json').write_text(json.dumps(manifest))
        (tmp / 'pair.json').write_text(json.dumps(pair))
        (tmp / 'document.json').write_text(json.dumps(document))
        if mutation is not None:
            (tmp / 'mutation.json').write_text(json.dumps(mutation))
        (tmp / 'scenario.json').write_text(json.dumps(scenario))
        (tmp / 'home').mkdir()
        evidence = tmp / 'evidence'
        env = dict(PATH=str(poison) + ':/usr/bin:/bin', HOME=str(tmp / 'home'), LANG='C', LC_ALL='C',
                   HERE=str(sim), SIM_REPO=str(REPO), ARM=row['arm'], INSTANCE='drill-r9d38-hermetic',
                   SIM_DRILL_NAME=NAME, SIM_EVIDENCE_DIR=str(tmp / 'drill-evidence'),
                   SIM_UPGRADE_SUPERVISOR=shared, SIM_UPGRADE_MANIFEST=str(tmp / 'document.json'),
                   SIM_UPGRADE_EVIDENCE=str(evidence), S8_PYTHON=str(wrapper),
                   R9D38_CLOCK=str(CLOCK), R9D38_REAL_LIB=str(REAL_LIB), R9D38_SCENARIO=str(tmp / 'scenario.json'),
                   R9D38_TRACE=str(tmp / 'trace.jsonl'), R9D38_POISON=str(tmp / 'poison.log'))
        result = subprocess.run([shared, '--', '/bin/sh', str(DRILL)], cwd=tmp, env=env, stdin=subprocess.DEVNULL,
                                capture_output=True, text=True, timeout=120)
        trace = [json.loads(line) for line in (tmp / 'trace.jsonl').read_text().splitlines()] \
            if (tmp / 'trace.jsonl').exists() else []
        receipt_file = json.loads((evidence / 'arm-receipt.json').read_text()) \
            if (evidence / 'arm-receipt.json').exists() else None
        poisoned = (tmp / 'poison.log').read_text() if (tmp / 'poison.log').exists() else ''
        return Run(result.returncode, result.stdout, result.stderr, trace, receipt_file, poisoned)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ── negative inputs ────────────────────────────────────────────────────────────────────────────
def set_command(op, rc):
    return lambda scenario: scenario['command_rc'].__setitem__(op, rc)


def set_scenario(**fields):
    return lambda scenario: scenario.update(fields)


def override_sample(op, **fields):
    return lambda scenario: scenario.setdefault('sample_overrides', {}).setdefault(op, {}).update(fields)


def owner_rows(moved, extra=()):
    rows = [dict(id=name, origin_owner=dict(ORIGIN), current_owner=dict(SUCCESSOR if name in moved else ORIGIN))
            for name in ('exec.original', 'flow.original')]
    return rows + [dict(id=name, origin_owner=dict(SUCCESSOR), current_owner=dict(SUCCESSOR)) for name in extra]


def flip_expect(key):
    return lambda manifest: manifest['expect'].__setitem__(key, not manifest['expect'][key])


def walked_until(op):
    def check(test, row, run):
        ops = operations(row)
        test.assertEqual(run.walked(), ops[:ops.index(op) + 1], 'the runner kept dispatching past a failed ' + op)
    return check


class Case:
    def __init__(self, name, want, reason, scenario=None, manifest=None, document=None, check=None):
        self.name, self.want, self.reason = name, want, reason
        self.scenario, self.manifest, self.document, self.check = scenario, manifest, document, check


def cases(row):
    family, ops, cli = row['family'], operations(row), int(row['cli_rc'])
    mutant = family == 'M'
    proofs = row['proofs'].split(',')
    # Common to every arm, each parameterised by the arm's OWN row.
    result = [
        Case('last_command_cli_rc_is_load_bearing', 4 if mutant else 3,
             'scenario_command_native_failure', scenario=set_command(ops[-1], 75 if cli == 0 else 0)),
        Case('own_proof_set_is_load_bearing', 4, 'scenario_proof_missing',
             scenario=set_scenario(judge_proofs=proofs[:-1])),
        Case('fixture_not_empty_after_stop_is_setup', 2, 'fixture_cleanup_not_proven',
             scenario=set_scenario(stop_remaining=[dict(pid=99, start_ticks=1)])),
    ]
    if family == 'A':
        result += [
            Case('two_300s_calibrations_must_fit_deadline', 4, 'calibration_does_not_fit_arm_deadline',
                 manifest=lambda manifest: manifest.__setitem__('deadline_s', 500)),
            Case('refused_calibration_never_samples_or_cuts', 3, 'scenario_command_native_failure',
                 scenario=set_command('calibrate-full', 75),
                 check=lambda test, row, run: (walked_until('calibrate-full')(test, row, run),
                                               test.assertEqual(run.events('clock')[-1]['slept'], 300.0))),
        ]
    elif family == 'R' and row['point'] == 'R0':
        result += [Case('pristine_cancel_keeps_original_owner', 3, 'r0_original_owner_changed',
                        scenario=override_sample('cancel', pid=4243))]
    elif family == 'R' and row['point'] == 'R1':
        result += [
            Case('preflight_without_partial_take_is_not_a_hit', 4, 'r1_partial_take_not_hit',
                 scenario=override_sample('partial-take', resources=owner_rows(('exec.original', 'flow.original')))),
            Case('reverse_cannot_reuse_forward_cut', 1, 'reverse_reused_forward_attempt',
                 scenario=override_sample('rollback', cut_id='cut-forward')),
            Case('reverse_cannot_reuse_forward_receipt', 1, 'forward_receipt_reused',
                 scenario=override_sample('rollback', control_receipt=dict(evidence_digest='forward-control'))),
        ]
    elif family == 'R' and row['point'] == 'R2':
        result += [
            Case('new_request_must_admit_a_new_resource', 4, 'r2_new_request_not_observed',
                 scenario=override_sample('new-request', resources=owner_rows(()))),
            Case('reverse_generation_must_be_next', 1, 'reverse_reused_forward_attempt',
                 scenario=override_sample('rollback', generation=4)),
        ]
    elif family == 'H':
        result += [
            Case('runner_cannot_skip_the_reverse_step', 2, 'scenario_operations_changed',
                 document=lambda document: document.__setitem__(
                     'operations', [step for step in document['operations'] if step['name'] != 'rollback'])),
            Case('refused_cold_boot_stops_before_reupgrade', 3, 'scenario_command_native_failure',
                 scenario=set_command('cold-boot', 1), check=walked_until('cold-boot')),
        ]
    elif family in ('G', 'C'):
        result += [Case('completion_expectation_is_bound_to_the_row', 2, 'fixture_completion_weakened',
                        manifest=flip_expect('completion'))]
        if family == 'G':
            result += [Case('refused_first_generation_stops_the_arm', 3, 'scenario_command_native_failure',
                            scenario=set_command(ops[0], 75), check=walked_until(ops[0]))]
        else:
            result += [Case('another_arms_judgment_cannot_sign', 1, 'scenario_judge_stale',
                            scenario=set_scenario(judge_arm='A-single'))]
    elif family == 'W':
        result += [
            Case('injector_ack_failure_is_incomplete', 4, 'fault_injector_native_failure',
                 scenario=set_scenario(inject_error=True), check=walked_until('inject')),
            Case('release_ack_failure_is_incomplete', 4, 'fault_injector_native_failure',
                 scenario=set_scenario(release_error=True), check=walked_until('release')),
            Case('runner_cannot_bypass_the_p26_injector', 2, 'fault_must_use_p26_injector',
                 document=lambda document: next(step for step in document['operations'] if step['name'] == 'inject')
                 .__setitem__('command', document['operations'][0]['command'])),
            Case('fault_arm_requires_its_hit_oracle', 2, 'fixture_required_oracle_changed',
                 manifest=flip_expect('hit_required')),
        ]
    elif family == 'I':
        result += [
            Case('judge_rc_cannot_be_laundered', 1, 'scenario_judge_rc_laundered',
                 scenario=set_scenario(judge_scenario_rc=3)),
            Case('stale_run_judgment_cannot_sign', 1, 'scenario_judge_stale',
                 scenario=set_scenario(judge_run_id='2' * 32)),
        ]
    elif family == 'M':
        result += [
            Case('surviving_mutant_is_never_green_or_killed', 4, 'mutant_did_not_turn_red',
                 scenario=set_scenario(judge_rc=0)),
            Case('unexecuted_mutant_bytes_cannot_sign_a_kill', 4, 'mutation_binary_not_executed',
                 scenario=set_scenario(mutation_not_executed=True)),
            Case('mutation_patch_must_be_bound_to_the_build', 2, 'mutation_not_built_from_pair',
                 document=lambda document: document.__setitem__('mutation_patch_digest', '9' * 64)),
        ]
    return result


class ArmControls(unittest.TestCase):
    """Per-arm two-sided controls; subclassed once per arm with ARM set."""
    ARM = None
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.row = table()[cls.ARM]
        cls.expected = expected_verdicts()

    def assertVerdict(self, run, want):
        # lib/assert.sh's red-evidence writer lists this instance's containers
        # (best effort, `|| true`); it lands on the poison too and is the only
        # docker call allowed. Any other poisoned call is a deployment effect.
        listing = re.compile(r'^\S+/poison/docker ps --filter label=sim\.instance=drill-r9d38-hermetic --format .*$')
        reached = [line for line in run.poison.splitlines() if not listing.match(line)]
        self.assertEqual(reached, [], 'a poisoned deployment command was reached')
        self.assertEqual(len(run.verdicts), 1, run.stderr[-3000:])
        verdict = run.verdicts[0]
        self.assertEqual((CODES.get(verdict[0]), int(verdict[1]), run.rc), (want, want, want), run.stderr[-3000:])
        arms = re.findall(r'^DRILL-ARM arm=(\S+) of=(\S+)$', run.stderr, re.M)
        self.assertEqual(arms, [(self.ARM, NAME)])
        programs = run.events('program')
        self.assertEqual([p['sha256'] for p in programs], [program_sha()], 'not the drill heredoc that ran')
        self.assertEqual(programs[0]['argv'][4], self.ARM)
        self.assertEqual(run.receipt_lines, 1, run.stdout)
        self.assertEqual(run.receipt['native_rc'], want)

    def test_arm_is_declared_by_drill_table_and_expectation(self):
        self.assertIn(self.ARM, header_arms())
        self.assertIn(self.ARM, self.expected, 'expected-verdicts.tsv has no row for this arm')
        self.assertEqual(CODES[self.expected[self.ARM]], int(self.row['expected_native']))
        ops = operations(self.row)
        self.assertEqual(len(ops), len(set(ops)), 'duplicate operation names would alias scenario controls')

    def test_positive_reaches_expected_native_verdict(self):
        row, want = self.row, CODES[self.expected[self.ARM]]
        run = run_arm(row)
        self.assertVerdict(run, want)
        receipt, ops = run.receipt, operations(row)
        self.assertEqual(receipt, run.receipt_file, 'shell-visible receipt differs from the persisted one')
        self.assertEqual((receipt['arm'], receipt['unit'], receipt['kind'], receipt['verdict']),
                         (self.ARM, NAME + '.' + self.ARM, row['kind'], RUNNER_VERDICTS[want]))
        self.assertEqual(receipt['reason'], 'all_declared_oracles_observed')
        self.assertEqual(receipt['mutant_verified'], row['family'] == 'M')
        self.assertEqual([c['operation'] for c in receipt['commands']], ops)
        self.assertEqual([c['expected_rc'] for c in receipt['commands']], [0] * (len(ops) - 1) + [int(row['cli_rc'])])
        self.assertTrue(all(c['native_rc'] == c['expected_rc'] for c in receipt['commands']))
        self.assertEqual(receipt['child_statuses'], [0] * len(ops) + [int(row['expected_native'])])
        self.assertEqual(run.walked(), ops, 'the runner did not walk exactly the declared operations')
        order = [row_['event'] for row_ in run.events('start', 'drain', 'produce_companion', 'judge', 'stop')]
        self.assertEqual(order, ['start', 'drain', 'produce_companion', 'judge', 'stop'])
        companion = run.events('produce_companion')[0]
        self.assertEqual((companion['arm'], companion['shared'], companion['timeout_ok']),
                         (self.ARM, os.environ['SIM_UPGRADE_SUPERVISOR'], True))
        clock = run.events('clock')[-1]
        calibrating = [s for s in run.events('sample') if s['operation'].startswith('calibrate-')]
        if row['family'] == 'A':
            # Two real 300-second sampling intervals on the virtual clock: an
            # ACK cannot stand in for the interval, nor a phase marker.
            self.assertGreaterEqual(clock['slept'], 600.0)
            for op in ('calibrate-idle', 'calibrate-full'):
                self.assertGreaterEqual(sum(s['operation'] == op for s in calibrating), 61, op)
        else:
            self.assertEqual((clock['slept'], calibrating), (0.0, []))
        if row['family'] == 'M':
            observed = run.events('observe_mutation')
            self.assertEqual(len(observed), 1)
            # The mutated owner's bytes are read before the destructive cut.
            self.assertEqual(observed[0]['op'], 'prepare')

    def run_case(self, case):
        run = run_arm(self.row, case.scenario, case.manifest, case.document)
        self.assertVerdict(run, case.want)
        self.assertIn(case.reason, run.receipt['reason'])
        self.assertNotEqual(case.want, CODES[self.expected[self.ARM]], 'a negative must leave the expected verdict')
        if case.check:
            case.check(self, self.row, run)


def arm_suite(arm):
    namespace = dict(ARM=arm)
    for case in cases(table()[arm]):
        namespace['test_negative_' + case.name] = (lambda case: lambda self: self.run_case(case))(case)
    klass = type('Drill38Arm_' + re.sub('[^A-Za-z0-9]', '_', arm), (ArmControls,), namespace)
    return unittest.defaultTestLoader.loadTestsFromTestCase(klass)


def fan_out(arms, results, jobs):
    """One isolated interpreter per arm, each leaving its own native receipt.

    r9d keeps one section per arm; each section reads only its own arm's rc and
    log. Concurrency is bounded so a small runner is slower, never oversubscribed.
    """
    from concurrent.futures import ThreadPoolExecutor
    results.mkdir(parents=True, exist_ok=False)

    def one(arm):
        with open(results / (arm + '.out'), 'w') as log:
            try:
                rc = subprocess.run([sys.executable, '-I', '-B', str(Path(__file__).resolve()), '--arm', arm],
                                    stdout=log, stderr=subprocess.STDOUT, timeout=600).returncode
            except subprocess.TimeoutExpired:
                rc = 124
        (results / (arm + '.rc')).write_text(str(rc) + '\n')
        return rc

    with ThreadPoolExecutor(max_workers=jobs) as pool:
        codes = list(pool.map(one, arms))
    return 0 if codes and all(code == 0 for code in codes) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--arm', required=True)
    parser.add_argument('--results', type=Path, help='fan out one interpreter per arm; receipts land here')
    parser.add_argument('--jobs', type=int, default=max(2, min(16, (os.cpu_count() or 2) // 2)))
    args = parser.parse_args()
    shared = os.environ.get('SIM_UPGRADE_SUPERVISOR', '')
    if not (shared.startswith('/') and os.access(shared, os.X_OK)):
        # The drill itself refuses to run without it; a skipped control is never a pass.
        print('drill 38 arm controls: shared FIX-ORPHAN supervisor (SIM_UPGRADE_SUPERVISOR) is required',
              file=sys.stderr)
        return 2
    arms = header_arms()
    if not arms or arms != list(table()):
        print('drill 38 arm controls: # arms: header and upgrade_tx_arms.tsv disagree', file=sys.stderr)
        return 1
    if args.arm != 'all' and args.arm not in arms:
        # An unknown selector must not run zero tests and pass vacuously.
        print('unknown drill 38 arm selector: ' + args.arm, file=sys.stderr)
        return 2
    selected = arms if args.arm == 'all' else [args.arm]
    if args.results:
        return fan_out(selected, args.results, max(1, args.jobs))
    suite = unittest.TestSuite(arm_suite(arm) for arm in selected)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() and result.testsRun and not result.skipped else 1


if __name__ == '__main__':
    sys.exit(main())
