#!/usr/bin/env python3
"""Two-sided arm controls for drills 99 and 100 (r9d-nonvacuity D-11 sections).

origin: hot-update S10 plan §9 / §10.2 P19 / P20; r9d D-11.

Every arm runs through its REAL drill shell (`ARM=<arm>`, the drill's own
shared-supervisor re-exec, its native case mapping and lib/assert.sh
drill_end), the REAL `fullstack_fixture.py run` (99: sealed-profile, step
manifest, P10 image binding, host/judge vocabulary, judge-result and
product-execution joins) and the REAL `fullstack_mutations.py run-arm` (100:
catalog validation, owner patch, presealed cells, per-phase receipt and 0->1->0
triplet checks). Only the deployment boundary is a double, through the
sandbox's `python3` PATH entry and the drill's own SIM_FULLSTACK_HOST/JUDGE
seams (tests/hot_update_fullstack_arm_boundary.py):

  * the P25 source-pinned rebuild of assemblycmd/supervise/evidencecmd;
  * for 99, the P10 whole-repository source archive (a declared candidate
    identity), the Go assembly host and the P01 judge (they replay documents);
  * for 100, the P19/P25 mutation assembly host (it writes phase receipts).

docker/ssh/sudo/systemctl/journalctl/tether are poisoned on PATH. Nothing here
is a fullstack deployment, cut, mutation kill or judge receipt. Each arm must
reach its expected native verdict from expected-verdicts.tsv on good evidence
for ITS OWN profile/row, and every negative must turn it to an exact other
native verdict through the exact guard it names (failed-require criteria are
recorded by the boundary shim). An arm whose oracle cannot leave its expected
verdict is the permanently-true family r9d exists for; one that cannot reach
it is a permanently-red arm nobody can sign.

Usage: python3 -I -B tests/hot-update-fullstack-arm-test.py --arm <99.ARM|100.ARM>|all
       [--results DIR] [--jobs N]
"""
import argparse
import hashlib
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
LIB = SIM / 'drills/lib'
BOUNDARY = Path(__file__).resolve().parent / 'hot_update_fullstack_arm_boundary.py'
NAMES = {'99': '99-hot-update-fullstack', '100': '100-hot-update-fullstack-controls'}
DRILLS = {key: SIM / 'drills' / (name + '.sh') for key, name in NAMES.items()}
PYTHON = '/usr/bin/python3'
POISONED = ('docker', 'ssh', 'sudo', 'systemctl', 'journalctl', 'tether')
CODES = {'GREEN': 0, 'ASSERT-FAIL': 1, 'SETUP-RED': 2, 'PRODUCT-RED': 3, 'INCOMPLETE': 4}
VERDICT = re.compile(r'^DRILL-VERDICT verdict=(\S+) rc=(\d+) assert_fail=(\d+) setup_red=(\d+) '
                     r'product_red=(\d+) not_covered=(\d+) nc_gap=(\d+) nc_guard=(\d+) pass=(\d+) -- ', re.M)
SECOND = 1_000_000_000
PINNED_DOUBLE = 'source_pinned_tools double'

sys.path.insert(0, str(LIB))
import fullstack_fixture as FX  # noqa: E402  real module: manifest, canonical, PREFIX only
import fullstack_mutations as FM  # noqa: E402  real module: raw catalog, function_body, oracle_digest


def canonical(value):
    return FX.canonical(value)


def header_arms(path):
    rows = re.findall(r'^# arms: (.*)$', path.read_text(), re.M)
    return rows[0].split() if len(rows) == 1 else []


def expected_verdicts():
    rows = {}
    for line in (SIM / 'expected-verdicts.tsv').read_text().splitlines():
        if line and not line.startswith('#'):
            fields = line.split('\t')
            rows[fields[0]] = fields[1]
    return rows


def catalog_rows():
    # The RAW catalog, not manifest(): the drill itself runs manifest() and a
    # stale catalog must stay visible as the drill's own SETUP-RED.
    return {row['id']: row for row in FM.load(LIB / 'fullstack_mutations.json')['mutations']}


def arm_keys():
    """Arm keys from the real drill headers, reconciled with both catalogs."""
    a99, a100 = header_arms(DRILLS['99']), header_arms(DRILLS['100'])
    manifest = FX.decode((LIB / 'fullstack_manifest.json').read_bytes())
    problems = []
    if not a99 or a99 != list(FX.ARMS) or [a['id'] for a in manifest['arms']] != a99 \
            or manifest['required_children'] != a99:
        problems.append('drill 99 arms drift from fullstack_fixture.ARMS / fullstack_manifest.json')
    if not a100 or a100 != list(FM.KEYS) or list(catalog_rows()) != a100:
        problems.append('drill 100 arms drift from fullstack_mutations.KEYS / fullstack_mutations.json')
    return ['99.' + a for a in a99] + ['100.' + a for a in a100], problems


def write_ref(path, value, schema):
    raw = canonical(value)
    path.write_bytes(raw)
    return dict(path=str(path), sha=hashlib.sha256(raw).hexdigest(), bytes=len(raw), schema=schema)


class Run:
    def __init__(self, process, stderr, criteria, poison, sandbox):
        self.rc, self.stderr, self.criteria, self.poison, self.sandbox = process, stderr, criteria, poison, sandbox
        rows = VERDICT.findall(stderr)
        self.verdict = rows[0][0] if len(rows) == 1 and len(re.findall(r'^DRILL-VERDICT\b', stderr, re.M)) == 1 else None
        self.counters = list(map(int, rows[0][2:])) if self.verdict else []
        self.arm_lines = re.findall(r'^DRILL-ARM arm=(\S+) of=(\S+)$', stderr, re.M)

    def failed(self):
        return [c for c in self.criteria if c != PINNED_DOUBLE]


class Sandbox:
    """One private directory per drill invocation; poisoned deployment tools."""

    def __init__(self, test, key):
        self.dir = Path(tempfile.mkdtemp(prefix='r9d-fullstack-' + key.replace('.', '-') + '-'))
        test.addCleanup(shutil.rmtree, self.dir, True)
        self.bin = self.dir / 'bin'
        for name in ('bin', 'home', 'evidence', 'tmp', 'scenario'):
            (self.dir / name).mkdir(mode=0o700)
        for tool in POISONED:
            self.script(self.bin / tool, '#!/bin/sh\nprintf "%s %s\\n" "${0##*/}" "$*" >> "$NV_FS_POISON"\nexit 97\n')
        # Exactly the two drill calls are interposed; every other python3 runs.
        self.script(self.bin / 'python3', '#!/bin/sh\n'
                    'case "${3:-}" in\n'
                    '    */drills/lib/fullstack_fixture.py|*/drills/lib/fullstack_mutations.py)\n'
                    '        if [ "${1:-}" = -I ] && [ "${2:-}" = -B ]; then\n'
                    '            exec ' + PYTHON + ' -I -B "$NV_FS_BOUNDARY" shim "$@"\n'
                    '        fi ;;\n'
                    'esac\n'
                    'exec ' + PYTHON + ' "$@"\n')
        for mode in ('host99', 'judge99', 'host100'):
            self.script(self.dir / mode, '#!/bin/sh\nexec ' + PYTHON + ' -I -B "' + str(BOUNDARY) + '" ' + mode + ' "$@"\n')

    @staticmethod
    def script(path, text):
        path.write_text(text)
        path.chmod(0o700)

    def run(self, key, extra, timeout=120):
        drill, arm = key.split('.', 1)
        supervisor = os.environ['SIM_UPGRADE_SUPERVISOR']
        env = dict(PATH=str(self.bin) + ':/usr/bin:/bin', HOME=str(self.dir / 'home'), LANG='C', LC_ALL='C',
                   TMPDIR=str(self.dir / 'tmp'), HERE=str(SIM), ARM=arm, INSTANCE='drill-r9d-' + drill + '-' + arm,
                   SIM_DRILL_NAME=NAMES[drill], SIM_EVIDENCE_DIR=str(self.dir / 'evidence'),
                   SIM_UPGRADE_SUPERVISOR=supervisor, NV_FS_BOUNDARY=str(BOUNDARY),
                   NV_FS_POISON=str(self.dir / 'poison.log'), NV_FS_CRITERIA=str(self.dir / 'criteria.log'))
        env.update(extra)
        with subprocess.Popen(['/bin/sh', str(DRILLS[drill])], cwd=self.dir, env=env, stdin=subprocess.DEVNULL,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
            try:
                _, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                # TERM lets the shared supervisor cancel and reap its lineage.
                process.terminate()
                try:
                    _, stderr = process.communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    _, stderr = process.communicate()
                stderr += '\nr9d: drill timed out\n'
        read = lambda name: (self.dir / name).read_text().splitlines() if (self.dir / name).exists() else []
        return Run(process.returncode, stderr, read('criteria.log'), read('poison.log'), self)


class ArmCase(unittest.TestCase):
    KEY = ''
    PROBLEMS = []

    def setUp(self):
        self.assertFalse(self.PROBLEMS, 'arm set drift: ' + '; '.join(self.PROBLEMS))
        self.drill, self.arm = self.KEY.split('.', 1)
        self.expected = expected_verdicts().get(NAMES[self.drill] + '.' + self.arm)
        self.assertIn(self.expected, CODES, 'expected-verdicts.tsv has no row for ' + self.KEY)

    def native(self, run, verdict, criterion=None):
        detail = '\n'.join(run.stderr.splitlines()[-25:]) + '\ncriteria=' + repr(run.criteria)
        self.assertEqual(run.verdict, verdict, detail)
        self.assertEqual(run.rc, CODES[verdict], detail)
        self.assertEqual(run.arm_lines, [(self.arm, NAMES[self.drill])], detail)
        # The drill harness may probe `docker ps` while recording a red; no
        # product, ssh, systemd or installed-tether call may ever happen.
        self.assertTrue(all(line.startswith('docker ps ') for line in run.poison), run.poison)
        if criterion is not None:
            self.assertIn(criterion, run.failed(), detail)


# ───────────────────────────── drill 99 ─────────────────────────────

def keys99(topology):
    keys = [dict(role='agent', host='agt1' if i < 2 else 'agt2', unit='tether-agent-a%d.service' % i,
                 sid='s10-' + ('small-push', 'small-pull', 'large-push', 'large-pull')[i],
                 nid='a%d' % i, domain='/opt/s10/a%d' % i, uid=2100 + i) for i in range(4)]
    for role in ('broker', 'nats', 'caddy'):
        for i in range(3 if topology == 'quorum' else 1):
            keys.append(dict(role=role, host='brk%d' % (i + 1), unit=role + '.service', sid='', nid='b%d' % i,
                             domain='/opt/s10/' + role, uid=2200))
    return keys


def profile99(arm, root, snapshot):
    """A sealed deploy profile whose steps follow the REAL manifest rows."""
    manifest = FX.decode((LIB / 'fullstack_manifest.json').read_bytes())
    topology = next(a for a in manifest['arms'] if a['id'] == arm)['topology']
    keys = keys99(topology)
    artifacts = [dict(id='T%d-%s' % (g, role), role=role, release='s10-%d' % g,
                      binary=hashlib.sha256(('%s-%d' % (role, g)).encode()).hexdigest(),
                      source_commit=snapshot['source_commit'], tree=snapshot['tree_digest'], dirty=snapshot['dirty_digest'])
                 for g in range(6) for role in ('agent', 'broker', 'ctl')]
    artifacts += [dict(id=v + '-' + role, role=role, release=v, binary=hashlib.sha256((role + v).encode()).hexdigest())
                  for role in ('nats', 'caddy') for v in ('A', 'B')]
    steps, previous = [], []
    for row in manifest['rows']:
        for role, direction in row['actions']:
            group = []
            column = 0 if role in ('agent', 'broker') else 1
            for key in (k for k in keys if k['role'] == role):
                step = dict(id='%s/%s/%s/%s' % (row['id'], role, key['host'], key['unit']), parent_id=row['id'],
                            action='rollback' if direction == 'reverse' else 'upgrade', direction=direction,
                            cohort=row['cohort'], leg=row['leg'], target=dict(key), depends_on=list(previous),
                            deadline_ns=12001 * SECOND)
                step.update({'from': row['from'][column] + '-' + role, 'to': row['to'][column] + '-' + role})
                group.append(step['id'])
                steps.append(step)
            previous = group
    return dict(id=FX.PREFIX + arm, topology=topology, regime='quiet', tier='deploy', evidence_root=str(root),
                command_bindings={}, probe_rate=10, start_ns=SECOND, end_ns=12001 * SECOND,
                budget_digest='2' * 64, workload_digest='1' * 64, required=keys, steps=steps, artifacts=artifacts)


def distinctive99(arm, profile):
    """The runtime only THIS arm's topology/rounds require (dropped by negatives)."""
    manifest = FX.decode((LIB / 'fullstack_manifest.json').read_bytes())
    rounds = next(a for a in manifest['arms'] if a['id'] == arm)['rounds']
    if profile['topology'] == 'quorum':
        key = next(k for k in profile['required'] if k['role'] == 'broker' and k['host'] == 'brk3')
    else:
        key = next(k for k in profile['required'] if k['nid'] == 'a3')
    candidates = [s for s in profile['steps'] if s['target'] == key and (not rounds or s['parent_id'] in rounds)]
    return key, candidates[-1]


def scenario99(sandbox, arm, defect=''):
    root, sdir = sandbox.dir / 'root', sandbox.dir / 'scenario'
    root.mkdir(mode=0o700)
    snapshot = dict(source_commit='c' * 40, tree_digest='a' * 64, dirty_digest='b' * 64)
    (sdir / 'snapshot.json').write_text(json.dumps(snapshot))
    profile = profile99(arm, root, snapshot)
    key, step = distinctive99(arm, profile)
    if defect == 'step-missing':
        profile['steps'] = [s for s in profile['steps'] if s['id'] != step['id']]
    elif defect == 'topology-flip':
        profile['topology'] = 'single' if profile['topology'] == 'quorum' else 'quorum'
    elif defect == 'foreign-profile':
        manifest = FX.decode((LIB / 'fullstack_manifest.json').read_bytes())
        other = next(a['id'] for a in manifest['arms'] if a['id'] != arm and a['topology'] == profile['topology'])
        profile['id'] = FX.PREFIX + other
    (sdir / 'profile.json').write_bytes(canonical(profile))
    image = write_ref(sdir / 'image.json', [a for a in profile['artifacts'] if a['role'] in ('agent', 'broker', 'ctl')], 'image')
    (sdir / 'inputs.json').write_bytes(canonical({'image_output': image}))
    raw = write_ref(sdir / 'host-input.json', {'r9d': 'hermetic boundary input; no product run'}, 'fullstack-host-input')
    samples = []
    for index, required in enumerate(k for k in profile['required'] if k['role'] in ('agent', 'broker')):
        if defect == 'execution-missing' and required == key:
            continue
        binary = next(a['binary'] for a in profile['artifacts'] if a['id'] == 'T5-' + required['role'])
        stamp = dict(process=dict(key=required, boot_id='boot-r9d', pidns='pidns-r9d', instance_id='i%d' % index,
                                  pid=1000 + index, start_ticks=50 + index),
                     tx_id='tx-r9d', cut_id='cut-r9d', direction='forward', query_nonce='nonce-%d' % index,
                     scope_digest='d' * 64, generation=2, revision=1, sequence=10, at_boot_ns=SECOND)
        after = dict(stamp, sequence=11, at_boot_ns=2 * SECOND)
        executed = 'f' * 64 if defect == 'exe-differs' and required == key else binary
        document = dict(schema='fullstack-kernel-identity-v1', before=stamp, after=after,
                        original=dict(device=1, inode=100 + index, sha=executed),
                        current=dict(device=1, inode=100 + index, sha=executed),
                        product=dict(stamp=stamp, complete=True, exe_sha=executed))
        kernel = write_ref(sdir / ('kernel-%d.json' % index), document, 'fullstack-kernel-identity-v1')
        samples.append(dict(stamp=stamp, exe_sha=binary, raw=kernel))
    judge = dict(rc=0, verdict='GREEN', failures=[], missing=[], inputs=[raw])
    host_rc = 0
    if defect == 'judge-green-unmet':
        judge['failures'] = ['r9d unmet criterion']
    elif defect == 'judge-assert':
        judge.update(rc=1, verdict='ASSERT-FAIL', failures=['r9d assertion'])
    elif defect == 'judge-product':
        judge.update(rc=3, verdict='PRODUCT-RED', failures=['r9d product loss'])
    elif defect == 'host-out-of-vocabulary':
        host_rc = 7
    elif defect == 'host-incomplete':
        host_rc = 4
    (sdir / 'scenario.json').write_text(json.dumps(dict(arm=arm, root=str(root), host_rc=host_rc, judge_rc=judge['rc'],
                                                         judge=judge, evidence=dict(profile=profile, raw=[raw], samples=samples))))
    env = dict(SIM_FULLSTACK_ROOT=str(root), SIM_FULLSTACK_PROFILE=str(sdir / 'profile.json'),
               SIM_FULLSTACK_HOST=str(sandbox.dir / 'host99'), SIM_FULLSTACK_JUDGE=str(sandbox.dir / 'judge99'),
               NV_FS_SNAPSHOT=str(sdir / 'snapshot.json'))
    if defect != 'real-pinned':
        env['NV_FS_PINNED'] = 'double'
    return env


class Drill99Arm(ArmCase):
    def drive(self, defect=''):
        sandbox = Sandbox(self, self.KEY)
        return sandbox.run(self.KEY, scenario99(sandbox, self.arm, defect))

    def test_good_evidence_reaches_expected_native_verdict(self):
        run = self.drive()
        self.native(run, self.expected)
        self.assertEqual(run.failed(), [], 'a positive must not pass through any failed guard')
        self.assertIn(PINNED_DOUBLE, run.criteria, 'the real fixture never reached the tool seam')
        root = run.sandbox.dir / 'root'
        self.assertEqual((root / 'assembly.native.rc').read_text(), '0\n')
        child = FX.decode((root / 'fullstack-child.json').read_bytes())
        self.assertEqual((child['arm'], child['native_rc'], child['verdict']), (self.arm, 0, 'GREEN'))

    def test_missing_step_of_this_arms_runtime_is_setup_red(self):
        self.native(self.drive('step-missing'), 'SETUP-RED', 'required child runtime step missing')

    def test_profile_with_the_other_topology_is_setup_red(self):
        self.native(self.drive('topology-flip'), 'SETUP-RED', 'arm topology mismatch')

    def test_profile_of_another_arm_is_setup_red(self):
        self.native(self.drive('foreign-profile'), 'SETUP-RED', 'deploy profile mismatch')

    def test_green_judge_without_this_arms_product_execution_is_setup_red(self):
        self.native(self.drive('execution-missing'), 'SETUP-RED', 'product execution sample missing')

    def test_running_bytes_differing_from_the_artifact_is_setup_red(self):
        self.native(self.drive('exe-differs'), 'SETUP-RED', 'executing SHA differs from supplied artifact')

    def test_green_judge_with_unmet_criteria_is_setup_red(self):
        self.native(self.drive('judge-green-unmet'), 'SETUP-RED', 'GREEN with unmet criteria')

    def test_host_status_outside_native_vocabulary_is_setup_red(self):
        self.native(self.drive('host-out-of-vocabulary'), 'SETUP-RED',
                    'supervisor or host failed outside native verdict vocabulary')

    def test_judge_assertion_red_stays_assert_fail(self):
        run = self.drive('judge-assert')
        self.native(run, 'ASSERT-FAIL')
        self.assertEqual(run.failed(), [])

    def test_judge_product_red_stays_product_red(self):
        self.native(self.drive('judge-product'), 'PRODUCT-RED')

    def test_incomplete_host_stays_incomplete(self):
        self.native(self.drive('host-incomplete'), 'INCOMPLETE')

    def test_real_source_pinned_resolution_is_not_run_pending_p25(self):
        # No double: the actual rebuild prerequisite is still absent on this
        # candidate, and it must surface as INCOMPLETE, never a verdict.
        run = self.drive('real-pinned')
        self.native(run, 'INCOMPLETE')
        self.assertIn('NOT_RUN pending P25 source-pinned host', run.stderr)
        self.assertNotIn(PINNED_DOUBLE, run.criteria)


# ───────────────────────────── drill 100 ────────────────────────────

def unique_context(source, body, needle, avoid=None):
    """Smallest whole-line window around needle that is unique in source and body."""
    lines = body.split('\n')
    hits = [i for i, line in enumerate(lines) if needle in line and (avoid is None or avoid not in line)]
    if not hits:
        return None
    first, last = hits[0], hits[0]
    while True:
        before = '\n'.join(lines[first:last + 1])
        if source.count(before) == 1 and body.count(before) == 1:
            return before
        if last + 1 < len(lines):
            last += 1
        elif first > 0:
            first -= 1
        else:
            return None


def patch100(row, defect=''):
    path = REPO / row['source_path']
    raw = path.read_bytes()
    source = raw.decode()
    body = FM.function_body(source, row['function'], path.suffix)
    predicate = row['predicate']
    if row['id'] == 'M10S':
        before = predicate
        after = "None if request.slot.id == 'r9d-slot' else self.launch(request)"
        if defect == 'scheduler-edits':
            after = "None if request.slot.id == 'r9d-slot' or True else self.launch(request)"
    else:
        before = unique_context(source, body, predicate)
        if before is None:
            before = predicate  # stale binding: the drill's own catalog check will refuse it
        if path.suffix == '.py':
            after = before.replace(predicate, '(' + predicate + ' or None)', 1)
        else:
            after = before + '\n\t_ = r9dMutationHit'
    if defect == 'comment-only':
        after = before + ('  # r9d' if path.suffix == '.py' else ' // r9d')
    elif defect == 'unparsable-patch':
        after = before + ' ((('  # an owner handoff that is not Python at all
    elif defect == 'wrong-anchor':
        anchor = next((line for line in body.split('\n')[1:] if line.strip() and predicate not in line
                       and source.count(line) == 1), None)
        before = anchor if anchor is not None else before.replace(predicate, '')
        after = before + ('  # r9d' if path.suffix == '.py' else '\n\t_ = r9dMutationHit')
    return dict(schema='fullstack-mutation-patch-v1', mutation_id=row['id'], source_sha=hashlib.sha256(raw).hexdigest(),
                before=before, after=after)


def scenario100(sandbox, arm, defect=''):
    rows = catalog_rows()
    row = rows[arm]
    root, sdir = sandbox.dir / 'root', sandbox.dir / 'scenario'
    root.mkdir(mode=0o700)
    (sdir / 'patch.json').write_bytes(canonical(patch100(row, defect)))
    expect = sdir / 'expect'
    expect.mkdir(mode=0o700)
    schedule = canonical(dict(mutation_id=arm, slots=['%s-slot-%d' % (arm, n) for n in range(3)]))
    (expect / 'schedule.json').write_bytes(schedule)
    cells = [dict(topology=cell, schedule=dict(path='schedule.json', sha=hashlib.sha256(schedule).hexdigest(),
                                               bytes=len(schedule))) for cell in row['topologies']]
    (expect / 'expectations.json').write_bytes(canonical(dict(schema='fullstack-mutation-expectations-v1',
                                                              mutation_id=arm, cells=cells)))
    others = [r for r in rows.values() if r['id'] != arm]
    scenario = dict(defect=defect, dirty_digest='e' * 64, reader_digest='9' * 64,
                    oracle_digest=FM.oracle_digest(row),
                    other_window=next(r['window'] for r in others if r['window'] != row['window']),
                    other_assertion=next(r['expected_assertion'] for r in others
                                         if r['expected_assertion'] != row['expected_assertion']),
                    claimed_cell='single' if row['topologies'][0] == 'quorum' else 'quorum')
    (sdir / 'scenario.json').write_text(json.dumps(scenario))
    return dict(SIM_FULLSTACK_ROOT=str(root), SIM_FULLSTACK_MUTATION_PATCH=str(sdir / 'patch.json'),
                SIM_FULLSTACK_MUTATION_EXPECTATIONS=str(expect / 'expectations.json'),
                SIM_FULLSTACK_HOST=str(sandbox.dir / 'host100'), SIM_FULLSTACK_JUDGE=str(sandbox.dir / 'judge99'),
                NV_FS_PINNED='double', NV_FS_SCENARIO=str(sdir / 'scenario.json'))


class Drill100Arm(ArmCase):
    def setUp(self):
        super().setUp()
        self.row = catalog_rows()[self.arm]

    def drive(self, defect=''):
        sandbox = Sandbox(self, self.KEY)
        return sandbox.run(self.KEY, scenario100(sandbox, self.arm, defect))

    def pending_probe_sender(self):
        # origin: fullstack_mutations.prepare_patch M10C: the before_step seam
        # is a source binding only until P19/P25 supplies the probe sender.
        return self.arm == 'M10C'

    def test_good_evidence_reaches_expected_native_verdict(self):
        run = self.drive()
        if self.pending_probe_sender():
            # NOT_RUN pending S10 P19/P25: the honest hermetic result is
            # INCOMPLETE; it must never read as the expected kill or GREEN.
            self.native(run, 'INCOMPLETE')
            self.assertIn('NOT_RUN pending S10 P19/P25 exact targeted-cut probe sender', run.stderr)
            self.assertEqual(run.failed(), [])
            return
        self.native(run, self.expected)
        self.assertEqual(run.failed(), [], 'a positive must not pass through any failed guard')
        self.assertIn(PINNED_DOUBLE, run.criteria, 'the real driver never reached the tool seam')
        root = run.sandbox.dir / 'root'
        self.assertEqual((root / 'control.native.rc').read_text(), '1\n')
        for cell in self.row['topologies']:
            chain = FM.decode((root / 'control' / cell / 'chain.json').read_bytes())
            self.assertEqual((chain['mutation_id'], chain['topology'], chain['chain']), (self.arm, cell, [0, 1, 0]))

    def red(self, defect, criterion):
        self.native(self.drive(defect), 'SETUP-RED', criterion)

    def test_comment_only_patch_cannot_sign_a_kill(self):
        self.red('comment-only', 'only expected, comments or formatting changed')

    def test_patch_missing_this_rows_predicate_is_setup_red(self):
        self.red('wrong-anchor', 'patch misses bound predicate')

    def test_surviving_mutant_is_not_a_kill(self):
        self.red('surviving-mutant', 'native result is not 0->1->0 assertion chain')

    def test_translated_native_status_is_not_a_kill(self):
        self.red('native-translated', 'native result is not 0->1->0 assertion chain')

    def test_red_outside_this_rows_window_is_not_a_kill(self):
        self.red('wrong-window', 'red in wrong window or domain')

    def test_red_at_another_assertion_is_not_a_kill(self):
        self.red('other-assertion', 'red at another assertion')

    def test_unclassified_driver_failure_is_not_a_kill(self):
        # Python's default status for an uncaught exception is 1: exactly the
        # status the drill maps to this arm's expected ASSERT-FAIL kill. A
        # crash must surface as SETUP-RED (drill 100's `2)` branch), as drill
        # 38's runner already guarantees for its M arms.
        if not self.pending_probe_sender():
            self.native(self.drive('malformed-receipt'), 'SETUP-RED')
        if self.row['source_path'].endswith('.py'):
            self.native(self.drive('unparsable-patch'), 'SETUP-RED')

    def test_row_specific_receipt_obligation(self):
        if self.row['deploy_only']:
            self.red('hermetic-tier', 'hermetic receipt cannot sign managed infra')
        if len(self.row['topologies']) == 2:
            self.red('other-cell', 'receipt from another mutation or cell')
        if self.row['surface'] == 'product':
            self.red('overlay-missing', 'overlay did not enter nested build')
        else:
            self.red('restore-changed', 'mutant bytes absent or restore artifact changed')
        if self.arm == 'M10S':
            self.red('scheduler-edits', 'scheduler mutation edits expected or omits more than one slot')


def arm_suite(key, problems):
    drill = key.split('.', 1)[0]
    base = Drill99Arm if drill == '99' else Drill100Arm
    case = type('Drill%s_%s' % (drill, re.sub('[^A-Za-z0-9]', '_', key.split('.', 1)[1])), (base,),
                dict(KEY=key, PROBLEMS=problems))
    names = unittest.TestLoader().getTestCaseNames(case)
    if key == '100.M10C':
        # Only the guards M10C's real path can reach before its pending seam.
        names = [n for n in names if n in ('test_good_evidence_reaches_expected_native_verdict',
                                            'test_comment_only_patch_cannot_sign_a_kill',
                                            'test_patch_missing_this_rows_predicate_is_setup_red',
                                            'test_unclassified_driver_failure_is_not_a_kill')]
    return unittest.TestSuite(case(name) for name in names)


class StrictResult(unittest.TextTestResult):
    def addSkip(self, test, reason):
        # A skip is never evidence: it fails the arm.
        self.addFailure(test, (AssertionError, AssertionError('skipped: ' + reason), None))


def fan_out(keys, results, jobs):
    """One isolated interpreter per arm, each leaving its own native receipt."""
    from concurrent.futures import ThreadPoolExecutor
    results.mkdir(parents=True, exist_ok=False)

    def one(key):
        with open(results / (key + '.out'), 'w') as log:
            try:
                rc = subprocess.run([sys.executable, '-I', '-B', str(Path(__file__).resolve()), '--arm', key],
                                    stdout=log, stderr=subprocess.STDOUT, timeout=900).returncode
            except subprocess.TimeoutExpired:
                rc = 124
        (results / (key + '.rc')).write_text(str(rc) + '\n')
        return rc

    with ThreadPoolExecutor(max_workers=jobs) as pool:
        codes = list(pool.map(one, keys))
    return 0 if codes and all(code == 0 for code in codes) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--arm', required=True)
    parser.add_argument('--results', type=Path, help='fan out one interpreter per arm; receipts land here')
    parser.add_argument('--jobs', type=int, default=max(2, min(22, (os.cpu_count() or 2) // 2)))
    args = parser.parse_args()
    keys, problems = arm_keys()
    if args.arm != 'all' and args.arm not in keys:
        # An unknown selector must not run zero tests and pass vacuously.
        print('unknown fullstack arm selector: ' + args.arm, file=sys.stderr)
        return 2
    supervisor = os.environ.get('SIM_UPGRADE_SUPERVISOR', '')
    if not (Path(supervisor).is_absolute() and os.access(supervisor, os.X_OK)):
        print('fullstack arm controls: shared FIX-ORPHAN supervisor (SIM_UPGRADE_SUPERVISOR) is required', file=sys.stderr)
        return 2
    selected = keys if args.arm == 'all' else [args.arm]
    if args.results:
        return fan_out(selected, args.results, max(1, args.jobs))
    suite = unittest.TestSuite(arm_suite(key, problems) for key in selected)
    result = unittest.TextTestRunner(verbosity=2, resultclass=StrictResult).run(suite)
    return 0 if result.wasSuccessful() and result.testsRun and not result.skipped else 1


if __name__ == '__main__':
    sys.exit(main())
