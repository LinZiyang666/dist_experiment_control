"""Driver boundary and mutation receipt controls; no deployment receipts."""
from hot_update_transfer_doubles import *

class TransferArmControls(unittest.TestCase):
    ARM = None

    def setUp(self):
        self.box = Sandbox(self.ARM)
        self.addCleanup(self.box.close)

    def assertVerdict(self, outcome, verdict, rc, **counts):
        self.assertEqual((outcome.verdict, outcome.rc), (verdict, rc), outcome.describe())
        self.assertEqual(len(outcome.verdicts), 1, outcome.describe())
        for key, value in counts.items():
            self.assertEqual(outcome.counts[key], value, key + ': ' + outcome.describe())

    def assertBoundaryClean(self):
        calls = self.box.calls()
        self.assertFalse([line for line in calls if line.startswith('UNEXPECTED')], calls)
        self.assertFalse((self.box.fake / 'poisoned').exists(), 'a real deployment command was reached')

    # ── shared by every arm ──────────────────────────────────────────────────
    def test_headers_dispatch_and_unknown_arm(self):
        drill = self.box.drill
        source = (SIM / 'drills' / (NAMES[drill] + '.sh')).read_text()
        arms = re.findall(r'^# arms: (.*)$', source, re.M)
        self.assertEqual(len(arms), 1)
        arms = arms[0].split()
        self.assertEqual(arms, list(tm.MUTATIONS) if drill == '64' else tm.POSITIVE)
        self.assertIn(self.ARM, arms)
        for key in ('fixture', 'grows', 'worst', 'forgoes'):
            rows = re.findall(r'^# ' + key + r': (.*)$', source, re.M)
            self.assertEqual(len(rows), 1, key)
            self.assertEqual([cell.split('=')[0] for cell in rows[0].split()], arms, key)
        body = source.split('case "${ARM:?}" in', 1)[1].split('\nesac\n', 1)[0]
        routes = re.findall(r'^    (\w+)\) transfer_run (\w+)( grow_to_3)? ;;$', body, re.M)
        self.assertEqual([r[0] for r in routes], arms)
        for label, called, grow in routes:
            self.assertEqual(called, label)
            self.assertEqual(bool(grow), label == 'B')
        outcome = self.box.run_drill(arm='ZZ9')
        self.assertVerdict(outcome, 'SETUP-RED', 2)
        self.assertEqual(self.box.calls(), [])
        self.assertBoundaryClean()

    def test_native_codes_keep_their_class(self):
        for code, verdict in ((0, 'GREEN'), (1, 'ASSERT-FAIL'), (2, 'SETUP-RED'), (3, 'PRODUCT-RED'),
                              (4, 'INCOMPLETE'), (9, 'SETUP-RED'), (75, 'SETUP-RED')):
            outcome = self.box.run_functions('INSTANCE=drill-native\ndrill_begin native\n_AS_PASS=1\n'
                                             'transfer_native %d\ndrill_end\n' % code, ['transfer_native'])
            self.assertVerdict(outcome, verdict, ('GREEN', 'ASSERT-FAIL', 'SETUP-RED', 'PRODUCT-RED',
                                                  'INCOMPLETE').index(verdict))

    def test_driver_python_entries_start_under_isolated_mode(self):
        # The driver's exact interpreter flags. An isolated interpreter without
        # its sibling modules is a permanent SETUP-RED no hermetic import sees.
        for entry, argv in (('transfer_fixture.py', ['--help']), ('transfer_mutations.py', ['catalog'])):
            result = subprocess.run(['python3', '-I', '-B', str(self.box.here / 'drills/lib' / entry)] + argv,
                                    cwd=self.box.root, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, entry + ': ' + result.stderr)

    def assertScheduleShape(self, plan, arm):
        # Independent of requirements(): the plan §10.3 shape of this arm's
        # schedule. Generator and validator share requirements(), so a cell
        # dropped there would otherwise be accepted on both sides at once.
        steps = plan['steps']
        actions = [s['action'] for s in steps]
        if arm in ('R1', 'R2'):
            self.assertEqual(set(actions), {arm})
            self.assertEqual(sorted((s['layer'], s['tier'], s['direction']) for s in steps),
                             sorted((layer, tier, direction) for layer in ('agent', 'broker-store', 'broker-control')
                                    for tier in 'AB' for direction in ('push', 'pull')))
            return
        ladder = {'agent-forward': 30, 'broker-forward': 110, 'agent-rollback': 190, 'broker-rollback': 190}
        for leg in (1, 2):
            mine = [s for s in steps if s['leg'] == leg]
            outage = arm in ('W1', 'W2') and leg == int(arm[1])
            want = [a for a in ladder if not (outage and a.startswith('agent-'))]
            self.assertEqual([s['action'] for s in mine if s['action'] in ladder], want, 'leg %d' % leg)
            for s in mine:
                if s['action'] in ladder:
                    self.assertEqual((s['latest_s'], s['cohorts']), (ladder[s['action']], list(tm.COHORTS)))
            special = [s['action'] for s in mine if s['action'] not in ladder and s['action'] not in ('cancel', 'session-rm')]
            self.assertEqual(special, [arm] if arm in ('C1', 'C2', 'C3') or outage else [], 'leg %d' % leg)
        self.assertEqual(actions[:2] == ['cancel', 'session-rm'], arm == 'X')
        self.assertEqual(sum(a in ('cancel', 'session-rm') for a in actions), 2 if arm == 'X' else 0)
        for s in steps:
            if s['action'].startswith('broker-'):
                self.assertEqual(len(s['hops']), 3 if arm == 'B' else 1)

    def test_manifest_and_window_predicates_are_two_sided(self):
        arm, plan = self.ARM, self.box.plan
        self.assertScheduleShape(plan, arm)
        defects = {
            'drop-last-cell': lambda p: p['steps'].pop(),
            'drop-required-child': lambda p: p['required_children'].pop(),
            'mix-control-and-positive': lambda p: p.update(controls=not p['controls']),
            'shorten-watchdog': lambda p: p.update(watchdog_s=60),
            'drop-real-transport': lambda p: p.update(transport='nats'),
            'drop-signature-cell': lambda p: p['steps'].remove(signature_step(p, arm)),
            'leg-two-missing': lambda p: p.update(steps=[s for s in p['steps'] if s['leg'] != 2]),
            'broker-hop-missing': lambda p: p['brokers'].pop(),
        }
        if arm == 'X':
            defects['victim-borrows-survivor'] = lambda p: p['victims'][1].update(bucket=p['cohorts'][0]['bucket'])
        if arm in ('W1', 'W2'):
            defects['unwatched-rate'] = lambda p: p['cohorts'][0].update(rate_limit_bps=262144)
        if arm in tm.MUTATIONS:
            others = [m for m in tm.MUTATIONS if m != arm]
            defects['foreign-defect'] = lambda p: p['mutation'].update(defect=tm.MUTATIONS[others[0]][1])
            defects['missing-role'] = lambda p: p['mutation'].update(roles=p['mutation']['roles'][:-1])
            defects['default-window-changed'] = lambda p: p['mutation'].update(
                minimum_wait_s=0 if p['mutation']['minimum_wait_s'] else 125)
        for label, mutate in defects.items():
            broken = copy.deepcopy(plan)
            mutate(broken)
            with self.assertRaises(tm.ContractError, msg=label):
                tm.check_manifest(broken)
        # The drill's own assert_setup predicate: the CLI, not just the function.
        good, bad = self.box.root / 'good.json', self.box.root / 'bad.json'
        good.write_text(json.dumps(plan))
        broken = copy.deepcopy(plan)
        broken['steps'].remove(signature_step(broken, arm))
        bad.write_text(json.dumps(broken))
        for path, want in ((good, 0), (bad, 2)):
            result = subprocess.run(['python3', '-I', '-B', str(SIM / 'drills/lib/transfer_mutations.py'), 'validate',
                                     '--manifest', str(path)], capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, want, result.stderr)
        # Window readiness for this arm's signature cell: a ready window passes,
        # a finished/empty/late/wrong-leg one is coverage (4), a foreign one setup.
        step = signature_step(plan, arm)
        sample = dict(manifest_sha=sha(json.dumps(plan, sort_keys=True).encode()), sequence=1,
                      windows={step['id']: window_rows(plan, step)},
                      rollback_tasks={step['id']: dict(ready=True, tier=step.get('tier'), direction=step.get('direction'))})
        self.assertEqual(tm.check_sample(plan, sample, step['id']), 0)
        self.assertEqual(tm.check_sample(plan, dict(sample, windows={}), step['id']), 4)
        data = 'durable_bytes' in sample['windows'][step['id']][0]
        for field, value in (('complete', True), ('elapsed_s', step['latest_s'] + 1), ('leg', 3 - step['leg']),
                             ('ready', False), ('durable_bytes' if data else 'pending_responsibilities', 0)):
            broken = copy.deepcopy(sample)
            broken['windows'][step['id']][0][field] = value
            with self.assertRaises(tm.CoverageError, msg=field):
                tm.check_sample(plan, broken, step['id'])
        try:
            tm.check_sample(plan, dict(sample, manifest_sha='0' * 64), step['id'])
        except tm.CoverageError:
            self.fail('a sample from another manifest must be setup, not a coverage wait')
        except tm.ContractError:
            pass
        else:
            self.fail('a sample from another manifest was accepted')

    def test_wait_window_classifies_missed_and_foreign_windows(self):
        # Real transfer_wait_window + real sample CLI: an expired window is the
        # drill's INCOMPLETE, a sample from another manifest is SETUP, never a cut.
        step = signature_step(self.box.plan, self.ARM)
        ev = self.box.root / 'wait-ev'
        ev.mkdir()
        (ev / 'manifest.json').write_text(json.dumps(self.box.plan, sort_keys=True))
        (self.box.fake / 'manifest.sha').write_text(sha(json.dumps(self.box.plan, sort_keys=True).encode()) + '\n')
        body = ('INSTANCE=drill-wait\nTRANSFER_EV="%s"; TRANSFER_MANIFEST="$TRANSFER_EV/manifest.json"\n'
                'drill_begin wait\n_AS_PASS=1\ntransfer_wait_window "%s"\necho "wait-rc=$?"\ndrill_end\n') % (ev, step['id'])
        names = ['transfer_native', 'transfer_step', 'transfer_fixture', 'transfer_wait_window']
        self.box.scenario()
        ready = self.box.run_functions(body, names)
        self.assertIn('wait-rc=0', ready.stdout, ready.describe())
        self.assertVerdict(ready, 'GREEN', 0)
        expired = window_rows(self.box.plan, step, complete=True)
        self.box.set_sample(step['id'], self.box.sample_doc(step, rows=expired, window_expired=True))
        missed = self.box.run_functions(body, names)
        self.assertIn('wait-rc=1', missed.stdout, missed.describe())
        self.assertVerdict(missed, 'INCOMPLETE', 4, not_covered=1)
        self.box.set_sample(step['id'], dict(self.box.sample_doc(step), manifest_sha='0' * 64))
        foreign = self.box.run_functions(body, names)
        self.assertIn('wait-rc=1', foreign.stdout, foreign.describe())
        self.assertVerdict(foreign, 'SETUP-RED', 2, setup_red=1)

    def test_full_drill_reaches_expected_verdict_with_every_cell_cut(self):
        self.box.scenario()
        outcome = self.box.run_drill()
        self.assertBoundaryClean()
        expected = 'ASSERT-FAIL' if self.box.drill == '64' else 'GREEN'
        self.assertVerdict(outcome, expected, int(expected != 'GREEN'), setup_red=0, product_red=0, not_covered=0)
        self.assertIn('DRILL-ARM arm=%s of=%s' % (self.ARM, NAMES[self.box.drill]), outcome.stderr)
        ev = self.box.transfer_evidence()
        plan = json.loads((ev / 'manifest.json').read_text())
        self.assertEqual(self.box.events(), self.box.expected_events(plan))
        verbs = self.box.fixture_verbs()
        self.assertEqual(verbs[:2], ['start', 'sample'])
        self.assertEqual(verbs[-3:], ['drain', 'judge', 'stop'])
        self.assertEqual(verbs.count('judge'), 1)
        self.assertEqual((ev / 'judge.rc').read_text().strip(), str(int(expected != 'GREEN')))
        if self.box.drill == '64':
            receipt = json.loads((ev / 'control-receipt.json').read_text())
            self.assertEqual((receipt['mutation_id'], receipt['killed'], receipt['expected_native_rc'],
                              receipt['actual_native_rc']), (self.ARM, True, 1, 1))
            builds = [line for line in self.box.calls() if line.startswith('GOFLAGS=')]
            selector = (ev / 'mutation/goflags.txt').read_text().strip()
            self.assertEqual(builds, ['GOFLAGS=' + selector] * 2)
            self.assertIn('-overlay=', selector)
            self.assertNotEqual(plan['artifacts']['A']['sha256'], plan['artifacts']['B']['sha256'])


def positive_negative(arm):
    """The arm's own fault, driven away from GREEN through the real driver."""
    def test(self):
        box = self.box
        plan = box.plan
        if arm in ('A', 'A1'):
            box.scenario(rules=[('*node upgrade nid0 --url https://artifact.invalid/B *', 75)])
            outcome = box.run_drill()
            self.assertVerdict(outcome, 'INCOMPLETE', 4, not_covered=1, setup_red=0)
            self.assertEqual([e[:2] for e in box.events()], [('agent', 'nid0')])
        elif arm == 'B':
            box.scenario(rules=[('*cluster upgrade --broker-only*', 1)])
            outcome = box.run_drill()
            self.assertVerdict(outcome, 'INCOMPLETE', 4, not_covered=1, setup_red=0)
            self.assertEqual(box.events()[-1][0], 'roll')
            box.scenario(rules=[('* grow_to_3 4 1 0', 1)])
            outcome = box.run_drill()
            self.assertVerdict(outcome, 'SETUP-RED', 2)
            self.assertNotIn('start', box.fixture_verbs())
        elif arm in ('C1', 'C2'):
            step = signature_step(plan, arm)
            doc = box.sample_doc(step)
            doc['connections']['carrier' if arm == 'C1' else 'data'] = []
            box.set_sample(step['id'], doc)
            box.scenario()
            outcome = box.run_drill()
            self.assertVerdict(outcome, 'SETUP-RED', 2, setup_red=1)
            self.assertNotIn('kill-conn', [e[0] for e in box.events()])
        elif arm == 'C3':
            box.scenario(rules=[('* sctl brk1 restart nats-server', 1)])
            outcome = box.run_drill()
            self.assertVerdict(outcome, 'SETUP-RED', 2, setup_red=1)
            self.assertEqual(box.events()[-1], ('restart-nats',))
        elif arm in ('R1', 'R2'):
            # A refused import-barrier command is retained for the judge (it may
            # end unknown after a genuine rollback); the ladder still completes.
            box.scenario(rules=[('*node upgrade nid1 --url https://artifact.invalid/%s-agent-A-push *' % arm, 75)])
            retained = box.run_drill()
            self.assertVerdict(retained, 'GREEN', 0)
            self.assertEqual(box.events(), box.expected_events(plan))
            first = plan['steps'][0]['id']
            box.deployment['artifacts'].pop(first)
            box.save_deployment()
            box.scenario()
            outcome = box.run_drill()
            self.assertVerdict(outcome, 'SETUP-RED', 2, setup_red=1)
            self.assertEqual(box.events(), [])
        elif arm == 'X':
            box.scenario(rules=[('*exec ctl -- kill -INT 4242', 1)])
            outcome = box.run_drill()
            self.assertVerdict(outcome, 'SETUP-RED', 2, setup_red=1)
            self.assertEqual(box.events(), [('cancel', '4242')])
            # A refused `session rm` leaves its victim window unexercised.
            box.scenario(rules=[('*ctl -- session rm victim-sid1', 1)])
            outcome = box.run_drill()
            self.assertVerdict(outcome, 'INCOMPLETE', 4, not_covered=1, setup_red=0)
            self.assertEqual(box.events(), [('cancel', '4242'), ('session-rm', 'victim-sid1')])
            step = signature_step(plan, arm)
            doc = box.sample_doc(step)
            del doc['victims']
            box.set_sample(step['id'], doc)
            box.scenario()
            outcome = box.run_drill()
            self.assertVerdict(outcome, 'SETUP-RED', 2, setup_red=1)
            self.assertEqual(box.events(), [])
        elif arm in ('W1', 'W2'):
            box.set_sample(arm + '-release', dict(manifest_sha='@MANIFEST_SHA@', sequence=3, windows={},
                                                 watchdog={arm: dict(current_cut_observed=True, elapsed_s=119,
                                                                     pending_data=True)}))
            box.scenario()
            outcome = box.run_drill()
            self.assertVerdict(outcome, 'INCOMPLETE', 4, not_covered=1, setup_red=0)
            agents = [c['agent_node'] for c in plan['cohorts']]
            events = box.events()
            # Fault cleanup still heals every blackholed agent after the miss.
            self.assertEqual([e for e in events if e[0] in ('drop-syn', 'undrop-syn')],
                             [('drop-syn', n) for n in agents] + [('undrop-syn', n) for n in agents])
            # The outage's own forward cut is refused: one missed cut is one
            # coverage gap, never counted twice by the fault and the scheduler.
            nth = 1 if arm == 'W1' else 2
            box.scenario(rules=[('*node upgrade nid0 --url https://artifact.invalid/B *', 75, nth)])
            outcome = box.run_drill()
            self.assertVerdict(outcome, 'INCOMPLETE', 4, not_covered=1, setup_red=0)
            tail = [e for e in box.events() if e[0] in ('agent', 'drop-syn', 'undrop-syn')][-9:]
            self.assertEqual(tail[4][:2], ('agent', 'nid0'))
            self.assertEqual(tail[:4] + tail[5:], [('drop-syn', n) for n in agents] + [('undrop-syn', n) for n in agents])
        self.assertBoundaryClean()
        # The original judge is never skipped by an early break.
        self.assertEqual(box.fixture_verbs().count('judge'), 1 if arm != 'B' else 0)
    return test


def positive_judge(arm):
    def test(self):
        # The fixture's own verdict survives the driver unchanged.
        ev = self.box.root / 'judge-ev'
        ev.mkdir()
        body = ('INSTANCE=drill-judge\nTRANSFER_EV="%s"; TRANSFER_MANIFEST="$TRANSFER_EV/manifest.json"\nARM=%s\n'
                'drill_begin judge\n_AS_PASS=1\ntransfer_finish\ndrill_end\n') % (ev, arm)
        names = ['transfer_native', 'transfer_step', 'transfer_fixture', 'transfer_finish', 'transfer_cleanup']
        for judge, drain, verdict, rc in ((0, 0, 'GREEN', 0), (1, 0, 'ASSERT-FAIL', 1), (3, 0, 'PRODUCT-RED', 3),
                                          (4, 0, 'INCOMPLETE', 4), (0, 2, 'SETUP-RED', 2)):
            self.box.scenario(judge_rc=judge)
            self.box.write(self.box.fake / 'rc.drain', str(drain))
            outcome = self.box.run_functions(body, names)
            self.assertVerdict(outcome, verdict, rc)
            self.assertEqual((ev / 'judge.rc').read_text().strip(), str(judge))
            self.assertEqual((ev / 'judge.out').read_text(), (self.box.fake / 'judge.out').read_text())
    return test


def control_kill(arm):
    def test(self):
        box = self.box
        # in-process: the exact receipt relation for THIS mutation's owner,
        # roles and default window; anything else is not this arm's kill.
        with tempfile.TemporaryDirectory(prefix='transfer-kill-' + arm + '-') as directory:
            root = Path(directory)
            repo, ev = root / 'repo', root / 'ev'
            patch = production_patch(repo, arm)
            ev.mkdir()
            build = tm.apply_overlay(repo, ev / 'mutation', arm, patch)
            with self.assertRaises(tm.ContractError):
                tm.apply_overlay(repo, root / 'foreign', arm, production_patch(repo, arm, foreign_owner_path(arm)))
            (ev / 'mutation/tether').write_bytes(b'hermetic kill archive; never executed')
            artifact = sha((ev / 'mutation/tether').read_bytes())
            (ev / 'mutation/build.json').write_text(json.dumps(dict(artifact_sha=artifact)))
            (ev / 'judge.out').write_text(native_line('ASSERT-FAIL', 1))
            (box.fake / 'archive.json').write_text('{}')
            subprocess.run(['python3', '-I', '-B', str(box.fake / 'archive.py'), str(box.fake / 'archive.json'), str(ev)],
                           check=True, timeout=30)
            receipt = tm.check_kill(ev, arm, 1)
            self.assertEqual((receipt['mutation_id'], receipt['killed']), (arm, True))
            self.assertEqual(build['roles'], tm.MUTATIONS[arm][2].split())
            for rc in (0, 2, 3, 4, 75):
                with self.assertRaises(tm.ContractError, msg='native %d' % rc):
                    tm.check_kill(ev, arm, rc)
            other = next(m for m in tm.MUTATIONS if m != arm)
            with self.assertRaises(tm.ContractError):
                tm.check_kill(ev, other, 1)
            runtime = json.loads((ev / 'mutation-runtime.json').read_text())
            witness = json.loads((ev / 'mutation-witness.json').read_text())
            source = json.loads((ev / 'mutation/source.json').read_text())
            spare = next((r for r in ('ctl', 'agent', 'broker') if r not in build['roles']), 'fixture')
            variants = {
                'role-missing': ('mutation-runtime.json', dict(runtime, processes=runtime['processes'][:-1])),
                'role-extra': ('mutation-runtime.json', dict(runtime, processes=runtime['processes'] + [
                    dict(runtime['processes'][0], role=spare)])),
                'not-running-mutant': ('mutation-runtime.json', dict(runtime, processes=[
                    dict(p, running_sha='d' * 64) for p in runtime['processes']])),
                'witness-other-mutation': ('mutation-witness.json', dict(witness, mutation_id=other)),
                'not-exercised': ('mutation-witness.json', dict(witness, exercised=False)),
                'foreign-owner': ('mutation/source.json', dict(source, sources=[
                    dict(source['sources'][0], path=foreign_owner_path(arm))])),
            }
            if arm in ('M02', 'M12B'):
                variants['default-window-shortened'] = ('mutation-witness.json', dict(witness, elapsed_s=124))
            for label, (name, value) in variants.items():
                original = (ev / name).read_bytes()
                (ev / name).write_text(json.dumps(value))
                try:
                    with self.assertRaises(tm.ContractError, msg=label):
                        tm.check_kill(ev, arm, 1)
                finally:
                    (ev / name).write_bytes(original)
            self.assertTrue(tm.check_kill(ev, arm, 1)['killed'])
            # driver level (real transfer_finish + kill CLI): a surviving or
            # unproven mutant never leaves a control receipt, and a surviving
            # one cannot reach the expected ASSERT-FAIL row.
            # The archive is bound to its own path (relocation is refused), so
            # every variant judges in place; the judge double regenerates the
            # runtime/witness rows from the real build each time.
            body = ('INSTANCE=drill-kill\nTRANSFER_EV="%s"; TRANSFER_MANIFEST="$TRANSFER_EV/manifest.json"\n'
                    'ARM=%s\ndrill_begin kill\n_AS_PASS=1\ntransfer_finish\ndrill_end\n') % (ev, arm)
            names = ['transfer_native', 'transfer_step', 'transfer_fixture', 'transfer_finish', 'transfer_cleanup']
            for judge, archive, verdict, rc, setup in ((0, {}, 'SETUP-RED', 2, 1), (4, {}, 'SETUP-RED', 2, 1),
                                                      (1, dict(drop_role=True), 'ASSERT-FAIL', 1, 1),
                                                      (1, dict(exercised=False), 'ASSERT-FAIL', 1, 1),
                                                      (1, {}, 'ASSERT-FAIL', 1, 0)):
                (ev / 'control-receipt.json').unlink(missing_ok=True)
                box.scenario(judge_rc=judge, archive=archive)
                outcome = box.run_functions(body, names)
                self.assertVerdict(outcome, verdict, rc, setup_red=setup)
                self.assertEqual((ev / 'control-receipt.json').exists(), setup == 0, outcome.describe())
        # A missing owner patch is refused before any build or business.
        (box.root / 'patches' / (arm + '.json')).unlink()
        box.scenario()
        outcome = box.run_drill()
        self.assertVerdict(outcome, 'SETUP-RED', 2)
        self.assertNotIn('start', box.fixture_verbs())
        self.assertFalse([line for line in box.calls() if line.startswith('GOFLAGS=')])
        self.assertBoundaryClean()
    return test


def control_reader(arm):
    def test(self):
        # A reader without transfer-v1 cannot be bound as this arm's mutant.
        self.box.write(self.box.fake / 'reader.B', CAPABLE_READER.replace('transfer-v1', 'legacy-v0'))
        self.box.scenario()
        outcome = self.box.run_drill()
        self.assertVerdict(outcome, 'SETUP-RED', 2)
        self.assertNotIn('start', self.box.fixture_verbs())
        self.assertBoundaryClean()
    return test


def arm_suite(arm):
    attributes = dict(ARM=arm)
    if arm in tm.MUTATIONS:
        attributes['test_control_kill_receipt_is_two_sided'] = control_kill(arm)
        attributes['test_control_reader_must_carry_transfer_v1'] = control_reader(arm)
    else:
        attributes['test_arm_fault_drives_verdict_away_from_green'] = positive_negative(arm)
        attributes['test_original_judge_status_survives'] = positive_judge(arm)
    case = type('TransferArm_' + arm, (TransferArmControls,), attributes)
    return unittest.defaultTestLoader.loadTestsFromTestCase(case)


