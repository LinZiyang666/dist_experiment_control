"""Adversarial controls for every frozen compatibility arm."""
from compat_drill_sandbox import *


class CompatArmControls(unittest.TestCase):
    ARM = None

    def setUp(self):
        self.sandbox = Sandbox(self.ARM)
        self.addCleanup(self.sandbox.close)
        self.case = CASES[self.ARM]

    def assertVerdict(self, outcome, verdict, message=''):
        code = VERDICTS.index(verdict)
        self.assertEqual(len(outcome.rows), 1, outcome.describe())
        self.assertEqual((outcome.verdict, outcome.rc, outcome.counts.get('rc')), (verdict, code, code),
                         message + '\n' + outcome.describe())
        self.assertIn('DRILL-ARM arm=%s of=%s' % (self.ARM, DRILL_NAME), outcome.stderr, outcome.describe())
        self.assertFalse((self.sandbox.fake / 'poisoned').exists(), 'deployment command reached: ' + outcome.describe())
        # PYCOMPAT always retains one receipt whose native status is the drill's.
        self.assertIsNotNone(outcome.receipt, outcome.describe())
        self.assertEqual(outcome.receipt['native_rc'], code, outcome.describe())

    def command_calls(self, outcome):
        return [line.split('\037') for line in outcome.sandbox.calls() if line.startswith('command ')]

    def negative(self, label, mutate, verdict, commands_ran=True):
        scenario = self.sandbox.scenario()
        mutate(scenario)
        outcome = self.sandbox.run(scenario)
        self.assertVerdict(outcome, verdict, 'negative ' + label)
        ran = bool(self.command_calls(outcome))
        self.assertEqual(ran, commands_ran, 'negative %s: product command execution %s\n%s' % (
            label, 'expected' if commands_ran else 'must not happen', outcome.describe()))

    def test_positive_reaches_expected_native_with_own_cli_statuses(self):
        expected = expected_verdict(self.ARM)
        self.assertEqual(self.case['cli'], EXPECTED_CLI.get(self.ARM), 'CASES39 CLI statuses drifted from the S8 pin')
        scenario = self.sandbox.scenario()
        outcome = self.sandbox.run(scenario)
        self.assertVerdict(outcome, expected)
        counts = dict(assert_fail=0, setup_red=0, product_red=0, not_covered=0, nc_gap=0, nc_guard=0, **{'pass': 1})
        if expected == 'INCOMPLETE':
            counts.update(not_covered=1, nc_guard=1, **{'pass': 0})
        self.assertEqual({k: outcome.counts[k] for k in counts}, counts, outcome.describe())
        receipt = outcome.receipt
        self.assertEqual((receipt['arm'], receipt['kind'], receipt['expected_native_rc'], receipt['expected_cli_rcs'],
                          receipt['command_rcs'], receipt['signature']),
                         (self.ARM, self.case['kind'], self.case['native'], self.case['cli'], self.case['cli'],
                          'hermetic-' + self.ARM), outcome.describe())
        # Every product command ran exactly once with its exact product argv.
        executed = self.command_calls(outcome)
        self.assertEqual(sorted((call[0], call[1:]) for call in executed),
                         sorted(('command %d' % slot, product) for slot, product in enumerate(products(self.ARM))),
                         outcome.describe())
        supplier = [line.split()[1] for line in outcome.sandbox.calls() if line.startswith('supplier ')]
        wanted = ['prepare'] + (['await-fence'] if self.ARM == 'CC-late-create' else []) + ['observe', 'judge', 'stop']
        self.assertEqual(supplier, wanted, outcome.describe())
        if self.ARM == 'ABI-adjacent':
            order = [line for line in outcome.sandbox.calls() if not line.startswith('command ')]
            self.assertEqual(order, ['supplier prepare ABI-adjacent', 'fixture manifest_read ABI-adjacent', 'fixture start',
                                     'supplier observe ABI-adjacent', 'fixture sample ABI-adjacent', 'fixture drain',
                                     'fixture judge', 'supplier judge ABI-adjacent', 'fixture stop',
                                     'supplier stop ABI-adjacent'], outcome.describe())
            calls = outcome.sandbox.calls()
            self.assertLess(calls.index('fixture start'), next(i for i, c in enumerate(calls) if c.startswith('command ')))
        if self.ARM == 'CC-late-create':
            calls = outcome.sandbox.calls()
            fence = calls.index('supplier await-fence CC-late-create')
            slots = [i for i, c in enumerate(calls) if c.startswith('command ')]
            # The competing create is issued only after the fence was observed
            # while the original upgrade was still live.
            self.assertLess(slots[0], fence)
            self.assertLess(fence, slots[1])

    def test_own_cli_status_is_load_bearing(self):
        for slot, rc in enumerate(self.case['cli']):
            wrong = 0 if rc else 64
            with self.subTest(slot=slot, wrong=wrong):
                def mutate(s, slot=slot, wrong=wrong):
                    s['cli'][str(slot)] = wrong
                self.negative('command %d status %d instead of %d' % (slot, wrong, rc), mutate, 'ASSERT-FAIL')

    def test_every_required_check_is_load_bearing(self):
        for key in required_checks(self.ARM):
            for how in ('false', 'missing'):
                with self.subTest(check=key, how=how):
                    def mutate(s, key=key, how=how):
                        if how == 'false':
                            s['checks'][key] = False
                        else:
                            del s['checks'][key]
                    self.negative('check %s %s' % (key, how), mutate, 'ASSERT-FAIL')

    def test_cleanup_that_leaves_owners_is_setup_red(self):
        def mutate(s):
            s['cleanup'] = dict(empty=True, remaining=[dict(pid=4100, start_ticks=9100)])
        self.negative('cleanup leaves a remaining owner', mutate, 'SETUP-RED')

    def test_execution_provenance_is_load_bearing(self):
        role = sorted(roles(self.ARM))[0]
        def mutate(s):
            del s['executions'][role]
        self.negative('role %s never executed' % role, mutate, 'SETUP-RED')
        def mutate_copy(s):
            s['executions'][role]['running_sha256'] = 'c' * 64
        self.negative('role %s running bytes differ from the copied artifact' % role, mutate_copy, 'SETUP-RED')


def wire_controls(arm):
    cell, verb = arm.split('-')

    def test_frozen_template_binds_this_cell_and_verb(self):
        other = 'run' if verb != 'run' else 'exec'
        def mutate(s):
            product = [bind(arg) for arg in template(cell, other)['argv']]
            s['plan']['commands'][0].update(product_argv=product, argv=[str(self.sandbox.fake / 'actor'), '--slot', '0', *product])
        self.negative('primary argv is the %s template' % other, mutate, 'SETUP-RED', commands_ran=False)
        foreign = cell[0] + ('1' if cell[1] == '0' else '0') + cell[2:]
        def mutate_cell(s):
            product = [bind(arg) for arg in template(foreign, verb)['argv']]
            s['plan']['commands'][0].update(product_argv=product, argv=[str(self.sandbox.fake / 'actor'), '--slot', '0', *product])
        self.negative('primary argv is the %s ctl quadrant template' % foreign, mutate_cell, 'SETUP-RED', commands_ran=False)
        # Every frozen template binds at least one sealed value; a sealed value
        # that differs from the one in the issued argv is a different command.
        bound = next(key for key in ('NONCE', 'PID', 'NID') if any(key in arg for arg in template(cell, verb)['argv']))
        def mutate_value(s):
            s['plan']['values'][bound] = 'f' * 32 if bound == 'NONCE' else '9999' if bound == 'PID' else 'agt9'
        self.negative('sealed %s differs from the bound argv' % bound, mutate_value, 'SETUP-RED', commands_ran=False)

    def test_wrong_quadrant_executable_is_setup_red(self):
        for role in ('ctl', 'agent', 'broker'):
            with self.subTest(role=role):
                def mutate(s, role=role):
                    path = flipped(s['executions'][role]['copied_path'])
                    row = execution_row(path)
                    # Self-consistent provenance of the OTHER quadrant: only the
                    # quadrant binding can reject it.
                    s['executions'][role] = row
                self.negative('%s executed from the flipped quadrant' % role, mutate, 'SETUP-RED')

    attributes = dict(test_frozen_template_binds_this_cell_and_verb=test_frozen_template_binds_this_cell_and_verb,
                      test_wrong_quadrant_executable_is_setup_red=test_wrong_quadrant_executable_is_setup_red)
    if verb in ('expose', 'transfer'):
        def test_companion_command_is_required(self):
            other = 'transfer_pull' if verb == 'expose' else 'expose_cleanup'
            def mutate(s):
                product = wire_manifest()['companions'][cell][other]
                s['plan']['commands'][1].update(product_argv=product, argv=[str(self.sandbox.fake / 'actor'), '--slot', '1', *product])
            self.negative('companion replaced by %s' % other, mutate, 'SETUP-RED', commands_ran=False)
            def mutate_drop(s):
                del s['plan']['commands'][1]
            self.negative('companion command dropped', mutate_drop, 'SETUP-RED', commands_ran=False)
        attributes['test_companion_command_is_required'] = test_companion_command_is_required
    return attributes


def config_controls(arm):
    _, component, selected = arm.split('-')

    def test_actual_parser_flags_are_required(self):
        flags = ('-t', '-c') if component == 'nats' else ('validate', '--config')
        for flag in flags:
            with self.subTest(flag=flag):
                def mutate(s, flag=flag):
                    product = [arg for arg in s['plan']['commands'][0]['product_argv'] if arg != flag]
                    s['plan']['commands'][0].update(product_argv=product, argv=[str(self.sandbox.fake / 'actor'), '--slot', '0', *product])
                self.negative('parser flag %s absent' % flag, mutate, 'SETUP-RED', commands_ran=False)

    def test_parser_role_is_the_executed_role(self):
        def mutate(s):
            s['executions'] = {role: execution_row('/opt/s8/' + selected + '/tether') for role in ('ctl', 'agent', 'broker')}
        self.negative('control roles substituted for the parser', mutate, 'SETUP-RED')

    return dict(test_actual_parser_flags_are_required=test_actual_parser_flags_are_required,
                test_parser_role_is_the_executed_role=test_parser_role_is_the_executed_role)


def interrupting_controls(arm):
    def test_interrupting_scope_is_exact(self):
        def mutate(s):
            command = s['plan']['commands'][0]
            product = list(command['product_argv'])
            if '--interrupting' in product:
                product.remove('--interrupting')
            else:
                product.append('--interrupting')
            command.update(product_argv=product, argv=[str(self.sandbox.fake / 'actor'), '--slot', '0', *product])
        label = 'interrupting flag dropped' if arm == 'LI-interrupting' else 'interrupting flag added'
        self.negative(label, mutate, 'SETUP-RED', commands_ran=False)
    return dict(test_interrupting_scope_is_exact=test_interrupting_scope_is_exact)


def late_create_controls():
    def test_late_create_requires_an_observed_live_fence(self):
        def missing(s):
            s['fence'] = 'missing'
        self.negative('fence never observed', missing, 'SETUP-RED')
        def early(s):
            s['modes']['0'] = 'early'
        self.negative('original upgrade finished before the fence', early, 'SETUP-RED')
        def unfenced(s):
            # The competing create still runs inside the observed fence, but
            # the product admits it: rc0 is an untracked admission, not 75.
            s['cli']['1'] = 0
        self.negative('late create admitted (rc0) inside the fence', unfenced, 'ASSERT-FAIL')
    return dict(test_late_create_requires_an_observed_live_fence=test_late_create_requires_an_observed_live_fence)


def abi_adjacent_controls():
    def test_full_profile_fixture_judge_and_custody_are_load_bearing(self):
        for rc, verdict in ((1, 'ASSERT-FAIL'), (3, 'PRODUCT-RED'), (4, 'INCOMPLETE'), (2, 'SETUP-RED')):
            with self.subTest(fixture_judge=rc):
                def mutate(s, rc=rc):
                    s['fixture']['judge_rc'] = rc
                self.negative('fixture judge native %d' % rc, mutate, verdict)
        def stop(s):
            s['fixture']['stop'] = 'fails'
        self.negative('fixture stop cannot prove an empty custody', stop, 'SETUP-RED')
        def foreign(s):
            self.sandbox.write(self.sandbox.fake / 'foreign-manifest.json', json.dumps(dict(arm='ABI-too-far', case_id='ABI-too-far')))
            s['plan']['fixture_manifest'] = str(self.sandbox.fake / 'foreign-manifest.json')
        self.negative('fixture manifest belongs to another arm', foreign, 'SETUP-RED', commands_ran=False)
    return dict(test_full_profile_fixture_judge_and_custody_are_load_bearing=test_full_profile_fixture_judge_and_custody_are_load_bearing)


def arm_suite(arm):
    attributes = dict(ARM=arm)
    kind = CASES[arm]['kind']
    if kind == 'wire':
        attributes.update(wire_controls(arm))
    elif kind == 'config':
        attributes.update(config_controls(arm))
    if kind not in ('wire', 'config'):
        attributes.update(interrupting_controls(arm))
    if arm == 'CC-late-create':
        attributes.update(late_create_controls())
    if arm == 'ABI-adjacent':
        attributes.update(abi_adjacent_controls())
    case = type('CompatArm_' + re.sub('[^A-Za-z0-9]', '_', arm), (CompatArmControls,), attributes)
    return unittest.defaultTestLoader.loadTestsFromTestCase(case)


