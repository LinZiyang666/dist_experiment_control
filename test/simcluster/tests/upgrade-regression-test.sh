#!/bin/sh
# origin: hot-update S8 plan §9.4 / §11.2 P29.
# Hermetic controls execute the actual embedded oracle with adversarial public
# evidence. No product cut, Docker, SSH or installed tether is exercised here.
set -eu
P29_SIM_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
export P29_SIM_ROOT
[ -x "${SIM_UPGRADE_SUPERVISOR:-}" ] || {
    printf 'regression tests: shared process supervisor is required\n' >&2; exit 2;
}
exec "$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B "$P29_SIM_ROOT/drills/lib/upgrade_tx_supervisor.py" run --timeout-s 55 -- /usr/bin/python3 -I -B - "$@" <<'PY'
import ast
import copy
import json
import os
from pathlib import Path
import shlex
import sys
import tempfile
import time
import types
import unittest

ROOT = Path(os.environ['P29_SIM_ROOT'])
SOURCE = ROOT / 'drills' / '31-node-upgrade-fleet.sh'
BEGIN, END = '# S8_REGRESSION_PYTHON_BEGIN\n', '# S8_REGRESSION_PYTHON_END'


def load_oracle():
    source = SOURCE.read_text().split(BEGIN, 1)[1].split(END, 1)[0]
    oracle = types.ModuleType('regression_oracle')
    args = sys.argv
    try:
        sys.argv = ['oracle', str(ROOT)]
        exec(compile(source, str(SOURCE), 'exec'), oracle.__dict__)
    finally:
        sys.argv = args
    return oracle


R = load_oracle()
C, S = R.C, R.S
SHA, OTHER, ID, CUT, NONCE = 'a' * 64, 'b' * 64, '1' * 32, '2' * 32, '3' * 32
OWNER = dict(boot_id='oracle-boot', pid=100, start_ticks=1000, instance_id='oracle-owner')
# Reuse P26's public schema fixture, not its runtime. Only these three pure
# constructors are loaded; no unittest, process launcher or fake port is run.
# origin: S10 internal review RUNALL-B F2. The wrapper now dispatches separate
# evidence and supervision modules; extract constructors from their owner.
fixture_source = (ROOT / 'tests' / 'upgrade_fixture_evidence.py').read_text()
constructors = {'synthetic_observation', 'synthetic_metrics', 'contract_rows'}
nodes = [node for node in ast.parse(fixture_source).body if isinstance(node, ast.FunctionDef) and node.name in constructors]
assert {node.name for node in nodes} == constructors
exec(compile(ast.Module(body=nodes, type_ignores=[]), '<public oracle fixture>', 'exec'), globals())
KEY = dict(domain_id='shared-install', component='agent', sid='lab', configured_nid='agt1', uid=1000)


def pair(label='A1'):
    sample = synthetic_observation()
    sample['arm'] = 'V2'
    handle = dict(key=copy.deepcopy(KEY), tx_id=ID, cut_id=CUT, generation=2, revision=2)
    for field, _ in R.RECEIPTS:
        receipt = sample[field]
        receipt['stamp']['attempt'] = copy.deepcopy(handle)
        receipt['census']['stamp'] = copy.deepcopy(receipt['stamp'])
    grant = dict(handle=copy.deepcopy(handle), source_sha=SHA, target_sha=OTHER, parent_tx_id='4'*32, parent_slot='agt1',
                 client_nonce=NONCE, issuer=copy.deepcopy(OWNER), actor_fp='public-actor', mode='hot',
                 source_release='same-tag', target_release='same-tag', scope_digest=SHA, capability_digest=OTHER, expires_at=900)
    forward = dict(handle=copy.deepcopy(handle), direction='forward', source_sha=SHA, target_sha=OTHER,
                   phase='completed', cut_stage='released', outcome='success')
    for field, sample_field in (('control', 'control_receipt'), ('resources', 'resources_receipt'), ('released', 'released_receipt')):
        forward[field] = copy.deepcopy(sample[sample_field])
    record = dict(version=2, key=copy.deepcopy(KEY), revision=2, grant=grant, forward=forward,
                  reverse=dict(direction='', handle=dict(key={}, tx_id='', cut_id='', generation=0, revision=0),
                               scope=dict(participants=None, expected=None), control={}, resources={}, released={}),
                  recorded_previous_sha=SHA, actions=[], reason='', effective_sha=OTHER)
    node = dict(key=copy.deepcopy(KEY), owner=copy.deepcopy(OWNER), incarnation='known-incarnation',
                online=True, probe_nonce=NONCE, probe_ok=True, running_sha=OTHER, effective_sha=OTHER, disk_sha=OTHER)
    row = dict(version=1, label=label, nonce=NONCE, at_ns=10, record=record,
               journal=dict(exists=True, durable=True, current=copy.deepcopy(record), archives=[], metadata=[]), node=node, plan=None)
    if label == 'B2':
        sample.update(phase='staged', outcome='pending')
        record['forward'].update(phase='staged', outcome='pending', cut_stage='none')
        record['actions'] = [dict(id='8'*32, nonce=NONCE, kind='publish', state='durable', handle=copy.deepcopy(handle),
                                  intent_digest=SHA, result_digest=OTHER)]
    if label in ('B3', 'C'):
        sample.update(phase='activating', outcome='pending')
        record['forward'].update(phase='activating', outcome='pending')
    if label in ('B6', 'B5', 'B6b', 'B6c', 'B6d'):
        sample.update(phase='rolled_back', outcome='rolled_back', direction='reverse', source_sha=OTHER,
                      target_sha=SHA, running_sha=SHA, effective_sha=SHA, generation=3, cut_id='5'*32)
        node.update(running_sha=SHA, effective_sha=SHA)
        reverse_handle = dict(handle, cut_id='5'*32, generation=3, revision=3)
        record['revision'] = 3
        record['reverse'] = dict(handle=copy.deepcopy(reverse_handle), source_sha=OTHER, target_sha=SHA, direction='reverse',
                                 phase='rolled_back', outcome='rolled_back')
        for field, _ in R.RECEIPTS:
            receipt = sample[field]
            receipt['running_sha'] = SHA
            receipt['stamp']['attempt'] = copy.deepcopy(reverse_handle)
            receipt['census']['stamp'] = copy.deepcopy(receipt['stamp'])
        for field, sample_field in (('control', 'control_receipt'), ('resources', 'resources_receipt'), ('released', 'released_receipt')):
            record['reverse'][field] = copy.deepcopy(sample[sample_field])
        if label == 'B6d':
            row['journal']['current'] = None
            row['journal']['archives'] = [copy.deepcopy(record)]
            row['journal']['metadata'] = [dict(tx_id=ID, archived_at=10, reported_revision=3)]
    if label != 'B6d':
        row['journal']['current'] = copy.deepcopy(record)
    expected = dict(key=copy.deepcopy(KEY), owner=copy.deepcopy(OWNER), incarnation='known-incarnation',
                    source_sha=SHA, target_sha=OTHER, parent_tx_id='4'*32, parent_slot='agt1')
    rc = 64 if label == 'B5' else 75 if label == 'C' else 0
    previous = {}
    if label == 'B5':
        previous['B3'] = dict(node=dict(running_sha=OTHER))
    if label == 'C':
        previous['B3'] = copy.deepcopy(row)
    if label == 'A3':
        previous['A1'] = copy.deepcopy(row)
    if label == 'C2':
        previous['A3'] = copy.deepcopy(row)
        record['key']['configured_nid'] = 'agt1b'
        grant['handle']['key'] = copy.deepcopy(record['key'])
        grant['handle']['tx_id'] = '6'*32
        sample['tx_id'] = '6'*32
        node['key'] = copy.deepcopy(record['key'])
        expected.update(key=copy.deepcopy(record['key']))
        record['forward']['handle'] = copy.deepcopy(grant['handle'])
        for field, _ in R.RECEIPTS:
            receipt = sample[field]
            receipt['stamp']['attempt'].update(tx_id='6'*32, key=copy.deepcopy(record['key']))
            receipt['census']['stamp'] = copy.deepcopy(receipt['stamp'])
        for field, sample_field in (('control', 'control_receipt'), ('resources', 'resources_receipt'), ('released', 'released_receipt')):
            record['forward'][field] = copy.deepcopy(sample[sample_field])
        row['journal']['current'] = copy.deepcopy(record)
        old = previous['A3']['record']
        row['journal']['archives'] = [copy.deepcopy(old)]
        row['journal']['metadata'] = [dict(tx_id=ID, archived_at=10, reported_revision=2)]
    return row, sample, rc, expected, previous


def fleet(label='fleet'):
    row, sample, rc, expected, previous = pair(label)
    grant = row['record']['grant']
    broker = dict(domain_id='shared-install', component='broker', configured_nid='brk1', config_path='/sandbox/broker.yaml', uid=1000)
    for item in (row['record'], row['record']['grant']['handle'], row['record']['forward']['handle'], row['node'], expected):
        item['key'] = copy.deepcopy(broker)
    for field, _ in R.RECEIPTS:
        sample[field]['stamp']['attempt']['key'] = copy.deepcopy(broker)
        sample[field]['census']['stamp'] = copy.deepcopy(sample[field]['stamp'])
    for field, sample_field in (('control', 'control_receipt'), ('resources', 'resources_receipt'), ('released', 'released_receipt')):
        row['record']['forward'][field] = copy.deepcopy(sample[sample_field])
    row['journal']['current'] = copy.deepcopy(row['record'])
    steps, children, receipts = [], [], []
    kinds = ['broker_cut'] + ([] if label in ('agentless', 'brokeronly') else ['agent_cut']) + ['home_converge', 'release']
    for ordinal, kind in enumerate(kinds, 1):
        participant = KEY if kind == 'agent_cut' else broker
        child_grant = copy.deepcopy(grant)
        child_grant['handle'].update(key=copy.deepcopy(participant), tx_id=str(ordinal)*32)
        child_grant['parent_slot'] = 'slot-' + str(ordinal)
        step = dict(ordinal=ordinal, participant=copy.deepcopy(participant), grant=child_grant, kind=kind,
                    depends_on=list(range(1, ordinal)))
        steps.append(step)
        receipt = copy.deepcopy(sample['released_receipt'])
        receipt['stamp']['attempt'] = copy.deepcopy(child_grant['handle'])
        if kind in ('broker_cut', 'agent_cut'):
            child = copy.deepcopy(sample)
            child['tx_id'] = child_grant['handle']['tx_id']
            for field, _ in R.RECEIPTS:
                child[field]['stamp']['attempt'] = copy.deepcopy(child_grant['handle'])
                child[field]['census']['stamp'] = copy.deepcopy(child[field]['stamp'])
            children.append(child)
        else:
            receipts.append(receipt)
    participants = [broker] + ([] if label in ('agentless', 'brokeronly') else [KEY])
    plan = dict(parent=dict(grant['handle'], tx_id='4'*32), scope=dict(id=ID, participants=participants, domains=sample['domains'], expected=contract_rows(sample)),
                steps=steps, source_manifest_digest=SHA, target_manifest_digest=OTHER, deadline=900)
    dispatches = [dict(ordinal=step['ordinal'], grant=copy.deepcopy(step['grant']), native_rc=0) for step in steps if step['kind'] in ('broker_cut', 'agent_cut')]
    fence = dict(current=copy.deepcopy(grant['handle']), owner=copy.deepcopy(OWNER), running_sha=SHA,
                 previous_sha=OTHER, incarnation='known-incarnation')
    row['plan'] = dict(before=copy.deepcopy(plan), after=copy.deepcopy(plan), dispatches=dispatches, step_receipts=receipts,
                       children=children, participants=[C.digest(key) for key in participants], holder='original-holder',
                       released_holder='original-holder', agents_before=[copy.deepcopy(fence)], agents_after=[copy.deepcopy(fence)],
                       driver_before=copy.deepcopy(OWNER), driver_after=dict(OWNER, pid=101, start_ticks=1001), driver_dead=True)
    if label == 'canary':
        rc = 74
        row['plan']['dispatches'] = row['plan']['dispatches'][:1]
        row['plan']['dispatches'][0]['native_rc'] = 74
    if label == 'noop':
        sample['source_sha'] = sample['target_sha']
    return row, sample, rc, expected, previous


def validate(label, values, drill='33'):
    row, sample, rc, expected, previous = values
    R.validate(drill, label, row, sample, rc, expected, NONCE, 1, 20, previous)


class UpgradeRegressionOracle(unittest.TestCase):
    # origin: hot-update S8 plan §9.4. Every assigned row has a green control.
    def test_each_v2_row_has_a_positive_control(self):
        for drill, labels in R.LABELS.items():
            for label in labels:
                with self.subTest(drill=drill, label=label):
                    validate(label, (pair if drill == '33' else fleet)(label), drill)

    def reject(self, label, mutate, reason, drill='33'):
        values = (pair if drill == '33' else fleet)(label)
        mutate(*values)
        with self.assertRaises(C.EvidenceError) as caught:
            validate(label, values, drill)
        self.assertIn(reason, str(caught.exception))
        self.assertIn(caught.exception.rc, (1, 3))

    # origin: hot-update S8 plan §11.2 P29 mandatory mutation.
    def test_deleted_journal_cannot_sign_committed(self):
        self.reject('A1', lambda r,s,c,e,p: r['journal'].update(exists=False, current=None, archives=[], metadata=[]), 'journal_deleted')

    def test_staged_phrase_requires_accepted_action_and_disk(self):
        def no_action(r,s,c,e,p):
            r['record'].update(actions=[])
            r['journal']['current'] = copy.deepcopy(r['record'])
        self.reject('B2', no_action, 'disk_stage')
        self.reject('B2', lambda r,s,c,e,p: r['node'].update(disk_sha=SHA), 'disk_stage')
        def completed(r,s,c,e,p):
            s.update(phase='completed')
            r['record']['forward']['phase'] = 'completed'
            r['journal']['current'] = copy.deepcopy(r['record'])
        self.reject('B2', completed, 'staged_phrase')

    def test_reverse_cannot_reuse_forward_or_preexec_state(self):
        def reused(r,s,c,e,p):
            r['record']['reverse']['handle'].update(cut_id=CUT)
            s['cut_id'] = CUT
            for field, _ in R.RECEIPTS:
                s[field]['stamp']['attempt']['cut_id'] = CUT
                s[field]['census']['stamp'] = copy.deepcopy(s[field]['stamp'])
            r['journal']['current'] = copy.deepcopy(r['record'])
        self.reject('B5', reused, 'reverse_not_fresh')
        def before_exec(r,s,c,e,p):
            s.update(running_sha=OTHER)
            r['node'].update(running_sha=OTHER)
        self.reject('B5', before_exec, 'completion_not_current')
        self.reject('B5', lambda r,s,c,e,p: s['released_receipt'].update(known=False), 'receipt_unknown')
        self.reject('B5', lambda r,s,c,e,p: p['B3']['node'].update(running_sha=SHA), 'without_forward_exec')
        def never_exec(r,s,c,e,p):
            s.update(running_sha=SHA)
            r['node'].update(running_sha=SHA)
        self.reject('B3', never_exec, 'never_executed')

    def test_online_requires_nonce_bound_control_subscription(self):
        for label in ('B6b', 'B6c', 'A1'):
            self.reject(label, lambda r,s,c,e,p: r['node'].update(probe_ok=False), 'online_without')

    def test_terminal_delivery_keeps_immutable_archive_and_metadata(self):
        self.reject('B6d', lambda r,s,c,e,p: r['journal'].update(archives=[]), 'execution_not_retained')
        self.reject('B6d', lambda r,s,c,e,p: r['journal'].update(current=r['record'], archives=[]), 'current_not_cleared')
        self.reject('B6d', lambda r,s,c,e,p: r['journal'].update(metadata=[]), 'metadata_missing')
        def missing_receipt(r,s,c,e,p):
            r['record']['reverse']['released']['complete'] = False
            r['journal']['archives'] = [copy.deepcopy(r['record'])]
        self.reject('B6d', missing_receipt, 'archived_receipt_missing')

    def test_completed_wait_needs_three_independent_receipts(self):
        for field, _ in R.RECEIPTS:
            self.reject('A1', lambda r,s,c,e,p,field=field: s[field].update(complete=False), 'receipt_unknown')
        self.reject('A1', lambda r,s,c,e,p: s['released_receipt'].update(evidence_digest=s['control_receipt']['evidence_digest']), 'not_independent')

    def test_target_key_owner_grant_parent_and_sha_are_bound(self):
        self.reject('A3', lambda r,s,c,e,p: r['node'].update(running_sha=SHA), 'running_owner')
        self.reject('A3', lambda r,s,c,e,p: r['node']['key'].update(configured_nid='other'), 'target_key')
        self.reject('A3', lambda r,s,c,e,p: r['node']['owner'].update(instance_id='other'), 'running_owner')
        self.reject('A3', lambda r,s,c,e,p: r['record']['grant'].update(parent_slot='other'), 'grant_or_parent')

    def test_competing_sibling_pending_defers_and_next_child_preserves_history(self):
        def current_changed(r,s,c,e,p):
            r['journal']['archives'].append(copy.deepcopy(r['record']))
            r['journal']['current'] = None if r['label'] == 'C' else copy.deepcopy(p['A3']['record'])
        self.reject('C', current_changed, 'sibling_pending')
        self.reject('C2', current_changed, 'late_callback')
        self.reject('C2', lambda r,s,c,e,p: r['journal']['archives'][0].update(revision=99), 'archive_missing')

    def test_full_plan_and_child_handles_survive_driver_resume(self):
        self.reject('resume', lambda r,s,c,e,p: r['plan']['after']['steps'][0]['grant']['handle'].update(tx_id='f'*32), 'ordered_plan', '31')
        self.reject('resume', lambda r,s,c,e,p: r['plan'].update(driver_dead=False), 'resume_not_observed', '31')
        self.reject('fleet', lambda r,s,c,e,p: r['plan'].update(participants=[]), 'participant_missing', '31')
        def shrink_saved_scope(r,s,c,e,p):
            for name in ('before', 'after'):
                r['plan'][name]['scope']['participants'].pop()
        self.reject('fleet', shrink_saved_scope, 'full_fleet_participant_missing', '31')

    def test_canary_failure_never_dispatches_held_children(self):
        self.reject('canary', lambda r,s,c,e,p: r['plan']['dispatches'][0].update(native_rc=0), 'canary_failure', '31')

    def test_agentless_and_broker_only_keep_agent_scope(self):
        def agent_step(r,s,c,e,p):
            for name in ('before', 'after'):
                r['plan'][name]['steps'][0]['kind'] = 'agent_cut'
        self.reject('agentless', agent_step, 'agentless_plan', '31')
        self.reject('brokeronly', lambda r,s,c,e,p: r['plan'].update(agents_after=[]), 'touched_agent', '31')
        self.reject('brokeronly', lambda r,s,c,e,p: r['plan']['agents_after'][0].update(running_sha=OTHER), 'touched_agent', '31')

    def test_noop_and_release_need_fresh_fleet_and_original_holder(self):
        self.reject('noop', lambda r,s,c,e,p: s.update(source_sha=SHA), 'noop_lacks', '31')
        self.reject('lease', lambda r,s,c,e,p: r['plan'].update(released_holder='foreign'), 'wrong_holder', '31')
        self.reject('fleet', lambda r,s,c,e,p: r['plan'].update(children=[]), 'child_completion', '31')

    def test_stale_observer_and_unobserved_authority_are_rejected(self):
        self.reject('A1', lambda r,s,c,e,p: r.update(nonce='f'*32), 'observation_stale')
        self.reject('A1', lambda r,s,c,e,p: r['node'].update(incarnation='foreign'), 'running_owner')

    def test_empty_journal_and_cli_outcomes_cannot_substitute_for_receipts(self):
        self.reject('A1', lambda r,s,c,e,p: r['journal'].update(current=None, archives=[]), 'execution_not_retained')
        values = pair('B5')
        values = (values[0], values[1], 0, values[3], values[4])
        with self.assertRaisesRegex(C.EvidenceError, 'rollback_cli_exit_wrong'):
            validate('B5', values)
        def wrong_target(r,s,c,e,p):
            r['record']['recorded_previous_sha'] = OTHER
            r['journal']['current'] = copy.deepcopy(r['record'])
        self.reject('A1', wrong_target, 'target_not_completed')

    def test_stage_slot_and_completed_history_cannot_be_replaced(self):
        def staged_archive(r,s,c,e,p):
            r['journal']['current'] = None
            r['journal']['archives'] = [copy.deepcopy(r['record'])]
        self.reject('B2', staged_archive, 'stage_current_missing')
        self.reject('A3', lambda r,s,c,e,p: p['A1']['record'].update(revision=99), 'completed_execution_changed')
        def reused_slot(r,s,c,e,p):
            p['A3']['record']['grant']['parent_slot'] = 'agt1b'
            r['journal']['archives'] = [copy.deepcopy(p['A3']['record'])]
        self.reject('C2', reused_slot, 'sibling_child_overwrote')

    def test_current_revision_is_bound_to_independent_observation(self):
        def overwritten(r,s,c,e,p):
            r['record']['revision'] += 1
            r['journal']['current'] = copy.deepcopy(r['record'])
        self.reject('C2', overwritten, 'current_attempt_not_observed')
        def stale_instance(r,s,c,e,p):
            for field, _ in R.RECEIPTS:
                s[field]['stamp']['producer']['instance_id'] = 'retired-instance'
                s[field]['census']['stamp'] = copy.deepcopy(s[field]['stamp'])
        self.reject('A3', stale_instance, 'current_attempt_not_observed')

    def test_go_zero_reverse_and_earlier_durable_receipts_are_valid(self):
        values = pair('C2')
        old = values[4]['A3']['record']
        old['forward']['handle']['revision'] = 3
        old['revision'] = 3
        values[0]['journal']['archives'] = [copy.deepcopy(old)]
        values[0]['journal']['metadata'][0]['reported_revision'] = 3
        validate('C2', values)

    def test_dispatch_and_child_completion_are_exact(self):
        self.reject('fleet', lambda r,s,c,e,p: r['plan']['dispatches'].reverse(), 'dispatch_incomplete_or_reordered', '31')
        def lost_cut(r,s,c,e,p):
            for name in ('before', 'after'):
                r['plan'][name]['steps'][1]['kind'] = 'home_converge'
        self.reject('fleet', lost_cut, 'fleet_cut_participant_missing', '31')
        self.reject('fleet', lambda r,s,c,e,p: r['plan']['dispatches'][0].update(grant={}), 'dispatch_not_original', '31')
        self.reject('fleet', lambda r,s,c,e,p: r['plan']['children'][0].update(tx_id='f'*32), 'child_receipt_changed', '31')
        def cached(r,s,c,e,p):
            child = r['plan']['children'][0]
            child['nonce'] = 'f'*32
            for field, _ in R.RECEIPTS:
                child[field]['stamp']['nonce'] = child['nonce']
                child[field]['census']['stamp'] = copy.deepcopy(child[field]['stamp'])
        self.reject('noop', cached, 'completion_stale', '31')
        self.reject('fleet', lambda r,s,c,e,p: r['plan']['step_receipts'].pop(), 'host_or_cluster_receipt_missing', '31')
        self.reject('fleet', lambda r,s,c,e,p: r['plan']['step_receipts'][0].update(known=False), 'host_or_cluster_receipt_unbound', '31')
        values = fleet('fleet')
        with self.assertRaisesRegex(C.EvidenceError, 'fleet_cli_exit_wrong'):
            validate('fleet', (values[0], values[1], 74, values[3], values[4]), '31')

    def test_private_fields_never_enter_journal_evidence(self):
        row = pair()[0]
        row['record']['salt'] = 'synthetic-secret-field'
        with self.assertRaisesRegex(C.EvidenceError, 'private_or_unknown_journal_field'):
            R.public_evidence(row)
        row = pair('B6d')[0]
        row['journal']['metadata'][0]['salt'] = 'synthetic-private-field'
        with self.assertRaisesRegex(C.EvidenceError, 'private_metadata_field'):
            R.public_evidence(row)

    def test_label_mismatch_cannot_sign_another_regression_row(self):
        self.reject('A1', lambda r,s,c,e,p: r.update(label='A3'), 'regression_label_changed')

    def test_owned_flat_arms_match_the_runner_manifest(self):
        with tempfile.TemporaryDirectory(prefix='p29-arms-') as directory:
            for name in ('31-node-upgrade-fleet.sh', '33-node-upgrade-success.sh'):
                (Path(directory) / name).write_text((ROOT / 'drills' / name).read_text())
            old = os.environ.get('DRILLS')
            try:
                os.environ['DRILLS'] = directory
                result = S.run(['/bin/sh', str(ROOT / 'tests' / 'arm-manifest-lint.sh')], 5)
                claims = S.run(['/bin/sh', str(ROOT / 'tests' / 'assert-identity.sh')], 5)
            finally:
                if old is None:
                    del os.environ['DRILLS']
                else:
                    os.environ['DRILLS'] = old
            self.assertEqual(result['native_rc'], 0, result['stdout'])
            self.assertEqual(claims['native_rc'], 0, claims['stdout'])
            self.assertNotIn('._shared\t', claims['stdout'])
            for name in ('31-node-upgrade-fleet', '33-node-upgrade-success'):
                for arm in ('V1', 'V2'):
                    self.assertIn(name + '.' + arm + '\tassert_ok\t', claims['stdout'])

    def test_native_verdicts_survive_the_shell_boundary(self):
        sources = [SOURCE.read_text(), (ROOT / 'drills' / '33-node-upgrade-success.sh').read_text()]
        bodies = ['\n_reg_rc=$1\ncase "$_reg_rc" in\n' + source.split('        case "$_reg_rc" in\n', 1)[1].split('        esac', 1)[0] + '\nesac' for source in sources]
        for body in bodies:
            for native in range(5):
                with tempfile.TemporaryDirectory(prefix='p29-verdict-') as directory:
                    script = '\n'.join(['exec 2>&1', 'INSTANCE=drill-p29-native-control', 'SIM_EVIDENCE_DIR=' + shlex.quote(directory),
                                        'HERE=' + shlex.quote(str(ROOT)), '. "$HERE/lib/log.sh"', '. "$HERE/lib/assert.sh"',
                                        'regression_native() {' + body + '\n}', 'drill_begin "p29-native-control"',
                                        'regression_native ' + str(native), 'drill_end'])
                    result = S.run(['/bin/sh', '-s'], 5, stdin_text=script)
                self.assertEqual(result['native_rc'], native, result['stdout'])
                self.assertIn('DRILL-VERDICT verdict=', result['stdout'])
                self.assertIn(' rc=' + str(native) + ' ', result['stdout'])

    def test_missing_adapter_is_setup_red_without_starting_fixture(self):
        with tempfile.TemporaryDirectory(prefix='p29-missing-') as directory:
            root = Path(directory)
            args = sys.argv
            try:
                sys.argv = ['oracle', str(ROOT), '33', str(root / 'absent.json'), str(root / 'evidence')]
                self.assertEqual(R.main(), 2)
            finally:
                sys.argv = args
            self.assertFalse((root / 'evidence').exists())

    def test_manifest_never_drops_a_regression_row(self):
        command = dict(argv=[sys.executable, '-c', 'raise SystemExit(2)'], cwd=str(ROOT), timeout_s=5)
        cases = [dict(label=label, command=command, deadline_s=600, want_rc=pair(label)[2], expect=pair(label)[3]) for label in R.LABELS['33']]
        manifest = dict(version=1, drill='33', fixture='/nonexistent/fixture.json', observer=command, cases=cases)
        with tempfile.TemporaryDirectory(prefix='p29-manifest-') as directory:
            path = Path(directory) / 'manifest.json'
            S.write_json(path, manifest)
            R.manifest_read(path, '33')
            manifest['cases'].pop()
            S.write_json(path, manifest)
            with self.assertRaises(C.EvidenceError):
                R.manifest_read(path, '33')

    def test_each_fleet_scope_requires_its_own_fixture(self):
        command = dict(argv=[sys.executable, '-c', 'raise SystemExit(2)'], cwd=str(ROOT), timeout_s=5)
        cases = [dict(label=label, command=command, deadline_s=600, want_rc=fleet(label)[2], expect=fleet(label)[3]) for label in R.LABELS['31']]
        manifest = dict(version=1, drill='31', fixture={label: '/nonexistent/' + label + '.json' for label in R.LABELS['31']},
                        observer=command, cases=cases)
        with tempfile.TemporaryDirectory(prefix='p29-fleet-manifest-') as directory:
            path = Path(directory) / 'manifest.json'
            S.write_json(path, manifest)
            R.manifest_read(path, '31')
            del manifest['fixture']['agentless']
            S.write_json(path, manifest)
            with self.assertRaisesRegex(C.EvidenceError, 'fleet_fixture_scope_missing'):
                R.manifest_read(path, '31')

    # origin: hot-update S8 plan §9.4. Historical shell assertions are kept
    # verbatim inside V1; a future migration must not silently delete them.
    def test_historical_v1_oracles_remain_separate(self):
        for file, needles in {
            '31-node-upgrade-fleet.sh': ('sha256_mismatch', 'url_not_allowed_local', 'F4c agt2 was NEVER dispatched'),
            '33-node-upgrade-success.sh': ('likely ROLLED BACK', '_b6_marker_gone', 'waiting for re-register (deadline 120s)', '_c2_committed'),
        }.items():
            source = (ROOT / 'drills' / file).read_text()
            body = source.split('    V1)\n', 1)[1].split('    V2)\n', 1)[0]
            for needle in needles:
                self.assertIn(needle, body)
            self.assertIn('\n        regression_v1_setup\n', source)
            self.assertIn('V2)', source)
            self.assertIn('exec "$SIM_UPGRADE_SUPERVISOR" -- /bin/sh', source)


class UpgradeRegressionSupervision(unittest.TestCase):
    # origin: hot-update S8 plan §11.2 P29. Five actual process exercises,
    # local FD baseline and the shared supervisor's empty descendant receipt.
    def test_process_commands_reap_in_five_rounds(self):
        baseline = len(list(Path('/proc/self/fd').iterdir()))
        for round in range(5):
            result = S.run([sys.executable, '-I', '-B', '-c', 'raise SystemExit(64)'], 2)
            self.assertEqual(result['native_rc'], 64)
        self.assertEqual(baseline, len(list(Path('/proc/self/fd').iterdir())))

    def test_cli_receipt_matches_joined_process_status(self):
        # A real short child exercises the CLI launcher protocol without a
        # product stub, fake transaction or fixture adapter.
        baseline = len(list(Path('/proc/self/fd').iterdir()))
        for round in range(5):
            with tempfile.TemporaryDirectory(prefix='p29-cli-join-') as directory:
                root = Path(directory)
                reader, writer = os.pipe()
                try:
                    S.start(root, reader, 30)
                    case = dict(label='B2', command=dict(argv=[sys.executable, '-I', '-B', '-c', 'raise SystemExit(64)'], cwd=directory, timeout_s=5))
                    R.cli_start(root, case)
                    self.assertEqual(R.cli_wait(root, 'B2', time.monotonic()+5), 64)
                    (root / 'cli-B2.rc').write_text('0\n')
                    with self.assertRaisesRegex(C.EvidenceError, 'cli_supervisor_native_status_changed'):
                        R.cli_wait(root, 'B2', time.monotonic()+5)
                finally:
                    os.close(reader)
                    os.close(writer)
                    S.wait_empty(root)
        self.assertEqual(baseline, len(list(Path('/proc/self/fd').iterdir())))


# Mutation receipts change only this package's owned source, on disk, then run
# the actual hermetic test through the shared supervisor. Every edit is restored
# in finally; compilation/setup failures never count as an assertion kill.
def mutations():
    selectors = {
        'journal_deleted_or_not_durable': 'test_deleted_journal_cannot_sign_committed',
        'journal_execution_not_retained': 'test_empty_journal_and_cli_outcomes_cannot_substitute_for_receipts',
        'regression_observation_stale': 'test_stale_observer_and_unobserved_authority_are_rejected',
        'regression_target_key_or_tx_changed': 'test_target_key_owner_grant_parent_and_sha_are_bound',
        'grant_or_parent_changed': 'test_target_key_owner_grant_parent_and_sha_are_bound',
        'running_owner_or_authority_changed': 'test_target_key_owner_grant_parent_and_sha_are_bound',
        'staged_phrase_without_acceptance': 'test_staged_phrase_requires_accepted_action_and_disk',
        'disk_stage_not_observed': 'test_staged_phrase_requires_accepted_action_and_disk',
        'stage_current_missing': 'test_stage_slot_and_completed_history_cannot_be_replaced',
        'staged_image_never_executed': 'test_reverse_cannot_reuse_forward_or_preexec_state',
        'completion_not_current': 'test_reverse_cannot_reuse_forward_or_preexec_state',
        'reverse_not_fresh': 'test_reverse_cannot_reuse_forward_or_preexec_state',
        'rollback_without_forward_exec': 'test_reverse_cannot_reuse_forward_or_preexec_state',
        'rollback_cli_exit_wrong': 'test_empty_journal_and_cli_outcomes_cannot_substitute_for_receipts',
        'online_without_command_subscription': 'test_online_requires_nonce_bound_control_subscription',
        'terminal_current_not_cleared': 'test_terminal_delivery_keeps_immutable_archive_and_metadata',
        'archive_missing_or_execution_rewritten': 'test_competing_sibling_pending_defers_and_next_child_preserves_history',
        'durable_report_metadata_missing': 'test_terminal_delivery_keeps_immutable_archive_and_metadata',
        'archived_receipt_missing': 'test_terminal_delivery_keeps_immutable_archive_and_metadata',
        'receipts_not_independent': 'test_completed_wait_needs_three_independent_receipts',
        'target_not_completed': 'test_empty_journal_and_cli_outcomes_cannot_substitute_for_receipts',
        'completed_execution_changed': 'test_stage_slot_and_completed_history_cannot_be_replaced',
        'sibling_pending_not_deferred': 'test_competing_sibling_pending_defers_and_next_child_preserves_history',
        'sibling_child_overwrote_current': 'test_stage_slot_and_completed_history_cannot_be_replaced',
        'late_callback_overwrote_new_current': 'test_competing_sibling_pending_defers_and_next_child_preserves_history',
        'ordered_plan_or_child_changed': 'test_full_plan_and_child_handles_survive_driver_resume',
        'full_fleet_participant_missing': 'test_full_plan_and_child_handles_survive_driver_resume',
        'dispatch_not_original_child': 'test_dispatch_and_child_completion_are_exact',
        'canary_failure_did_not_stop': 'test_canary_failure_never_dispatches_held_children',
        'fleet_cli_exit_wrong': 'test_dispatch_and_child_completion_are_exact',
        'fleet_child_completion_missing': 'test_noop_and_release_need_fresh_fleet_and_original_holder',
        'fleet_child_receipt_changed': 'test_dispatch_and_child_completion_are_exact',
        'fleet_child_completion_stale': 'test_dispatch_and_child_completion_are_exact',
        'host_or_cluster_receipt_missing': 'test_dispatch_and_child_completion_are_exact',
        'host_or_cluster_receipt_unbound': 'test_dispatch_and_child_completion_are_exact',
        'agentless_plan_has_agent_step': 'test_agentless_and_broker_only_keep_agent_scope',
        'broker_only_touched_agent': 'test_agentless_and_broker_only_keep_agent_scope',
        'noop_lacks_fresh_full_fleet_proof': 'test_noop_and_release_need_fresh_fleet_and_original_holder',
        'driver_resume_not_observed': 'test_full_plan_and_child_handles_survive_driver_resume',
        'current_attempt_not_observed': 'test_current_revision_is_bound_to_independent_observation',
        'fleet_cut_participant_missing': 'test_dispatch_and_child_completion_are_exact',
        'fleet_dispatch_incomplete_or_reordered': 'test_dispatch_and_child_completion_are_exact',
        'wrong_holder_released': 'test_noop_and_release_need_fresh_fleet_and_original_holder',
        'private_or_unknown_journal_field': 'test_private_fields_never_enter_journal_evidence',
        'private_metadata_field': 'test_private_fields_never_enter_journal_evidence',
        'regression_label_changed': 'test_label_mismatch_cannot_sign_another_regression_row',
        'regression_cases_missing_or_reordered': 'test_manifest_never_drops_a_regression_row',
        'cli_supervisor_native_status_changed': 'UpgradeRegressionSupervision.test_cli_receipt_matches_joined_process_status',
    }
    original = SOURCE.read_text()
    code = original.split(BEGIN, 1)[1].split(END, 1)[0]
    offset = original.index(BEGIN) + len(BEGIN)
    lines = code.splitlines(keepends=True)
    failures = []
    for reason, selector in selectors.items():
        calls = [node for node in ast.walk(ast.parse(code)) if isinstance(node, ast.Call) and len(node.args) >= 2
                 and isinstance(node.args[1], ast.Constant) and node.args[1].value == reason]
        assert len(calls) == 1, reason
        argument = calls[0].args[0]
        begin = offset + sum(map(len, lines[:argument.lineno-1])) + argument.col_offset
        end = offset + sum(map(len, lines[:argument.end_lineno-1])) + argument.end_col_offset
        try:
            SOURCE.write_text(original[:begin] + 'True' + original[end:])
            result = S.run(['/bin/sh', str(ROOT / 'tests' / 'upgrade-regression-test.sh'),
                            selector if '.' in selector else 'UpgradeRegressionOracle.' + selector], 10)
            killed = result['native_rc'] == 1 and 'FAILED (failures=' in result['stdout'] and 'ERROR:' not in result['stdout']
            print('P29_MUTATION ' + json.dumps(dict(guard=reason, native_rc=result['native_rc'], assertion_killed=killed)))
            if not killed:
                failures.append(reason)
                print(result['stdout'])
        finally:
            SOURCE.write_text(original)
    assert not failures, failures
    extra = [(SOURCE, 'result = dict(rc=error.rc,', 'result = dict(rc=0,', 'test_missing_adapter_is_setup_red_without_starting_fixture'),
             (SOURCE, "shape(manifest['fixture'], ' '.join(LABELS['31']), 'fleet_fixture_scope_missing')", 'None', 'test_each_fleet_scope_requires_its_own_fixture'),
             (SOURCE, 'case "${ARM:?}" in', 'case "$ARM" in', 'test_owned_flat_arms_match_the_runner_manifest'),
             (SOURCE, '3) product_red "v2 regression product oracle failed; evidence retained" ;;', '3) : ;;', 'test_native_verdicts_survive_the_shell_boundary'),
             (ROOT / 'drills' / '33-node-upgrade-success.sh', '3) product_red "v2 regression product oracle failed; evidence retained" ;;',
              '3) : ;;', 'test_native_verdicts_survive_the_shell_boundary')]
    for path, before, after, selector in extra:
        original_text = path.read_text()
        assert original_text.count(before) == 1
        try:
            path.write_text(original_text.replace(before, after))
            result = S.run(['/bin/sh', str(ROOT / 'tests' / 'upgrade-regression-test.sh'),
                            'UpgradeRegressionOracle.' + selector], 10)
            killed = result['native_rc'] == 1 and 'FAILED (failures=' in result['stdout'] and 'ERROR:' not in result['stdout']
            print('P29_MUTATION ' + json.dumps(dict(guard=path.name + ':' + selector, native_rc=result['native_rc'], assertion_killed=killed)))
            assert killed, result['stdout']
        finally:
            path.write_text(original_text)


if __name__ == '__main__':
    if sys.argv[1:] == ['--mutations']:
        mutations()
    else:
        unittest.main(testRunner=unittest.TextTestRunner(stream=sys.stdout, verbosity=2))
PY
