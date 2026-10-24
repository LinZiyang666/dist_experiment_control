#!/usr/bin/env -S python3 -I -B
"""Hermetic assembly controls. These are not real-product lifecycle receipts.

No drill, deployment command or child process runs here. P25 exercises the
command adapter and its real Start/Drain/Stop/Join owners under the leak gates.
origin: hot-update S10 plan §10.2 P19 / FullstackFixtureAssemblyTest
"""
import contextlib
import copy
import hashlib
import io
import json
from pathlib import Path
import runpy
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fullstack_fixture as fixture

S = 1_000_000_000


def profile(root, arm='A'):
    keys = [dict(role='agent', host='agt1' if i < 2 else 'agt2', unit=f'tether-agent-a{i}.service',
                 sid='s10-' + ('small-push', 'small-pull', 'large-push', 'large-pull')[i],
                 nid=f'a{i}', domain=f'/opt/s10/a{i}', uid=2100+i) for i in range(4)]
    quorum = arm in ('B', 'C3', 'S3', 'F3', 'K')
    for role in ('broker', 'nats', 'caddy'):
        for i in range(3 if quorum else 1):
            keys.append(dict(role=role, host=f'brk{i+1}', unit=role+'.service', sid='', nid=f'b{i}',
                             domain=f'/opt/s10/{role}', uid=2200))
    artifacts = []
    for generation in range(6):
        for role in ('agent', 'broker', 'ctl'):
            artifacts.append(dict(id=f'T{generation}-{role}', role=role, release=f's10-{generation}',
                                  binary=hashlib.sha256(str(generation).encode()).hexdigest()))
    for role in ('nats', 'caddy'):
        for version in ('A', 'B'):
            artifacts.append(dict(id=version+'-'+role, role=role, release=version,
                                  binary=hashlib.sha256(version.encode()).hexdigest()))
    result = dict(id=fixture.PREFIX+arm, topology='quorum' if quorum else 'single', tier='hermetic',
                  regime='quiet', evidence_root=str(root), start_ns=S, end_ns=12001*S, probe_rate=10,
                  workload_digest='1'*64, budget_digest='2'*64, required=keys, artifacts=artifacts, steps=[])
    chain = ['T0', 'T1', 'T0', 'T2', 'T3', 'T4', 'T5', 'T5']
    orders = [('agent', 'broker', 'nats', 'caddy'), ('caddy', 'nats', 'broker', 'agent'),
              ('agent', 'broker', 'nats', 'caddy'), ('caddy', 'nats', 'agent', 'broker'),
              ('agent', 'broker', 'nats', 'caddy'), ('caddy', 'nats', 'agent', 'broker'), ('nats', 'caddy')]
    previous = []
    for index, roles in enumerate(orders):
        for role in roles:
            group = []
            for key in (k for k in keys if k['role'] == role):
                reverse = index == 1 or index in (3, 5) and role in ('nats', 'caddy')
                step = dict(id=f'R{index+1}/{role}/{key["host"]}/{key["unit"]}', parent_id=f'R{index+1}',
                            action='rollback' if reverse else 'upgrade', direction='reverse' if reverse else 'forward',
                            cohort='C0' if index < 3 else 'C1', leg='leg1' if index in (0, 3) else 'leg2',
                            target=key.copy(), depends_on=previous.copy(), deadline_ns=result['end_ns'])
                versions = chain[index:index+2] if role in ('agent', 'broker') else ('A', 'B') if index % 2 == 0 else ('B', 'A')
                step.update({'from': versions[0]+'-'+role, 'to': versions[1]+'-'+role})
                group.append(step['id']); result['steps'].append(step)
            previous = group
    return result


def window(cohort='C0', leg='leg1'):
    originals, receipts = [], []
    for i in range(4):
        started = 10*S + i*S
        deadline = started + (2092 if i < 2 else 2108)*S
        originals.append(dict(runtime=f'a{i}', cohort=cohort, clock_id='fixture-clock', transfer_id=f'{cohort}-original-{i}', start_ns=started,
                              deadline_ns=deadline))
        receipts.append(dict(transfer_id=f'{cohort}-original-{i}', cohort=cohort, leg=leg, clock_id='fixture-clock',
                             role='sender' if leg == 'leg1' else 'receiver', deadline_ns=deadline,
                             first_durable_ns=started + (1 if leg == 'leg1' else 1030)*S,
                             ack_chunks=1, total_chunks=2000))
    return dict(originals=originals, receipts=receipts, clock_id='fixture-clock', at_ns=(20 if leg == 'leg1' else 1060)*S)


class FullstackFixtureAssemblyTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='fullstack-assembly-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.manifest = fixture.decode((fixture.HERE / 'fullstack_manifest.json').read_bytes())
        self.profile = profile(self.root)

    def ref(self, name, value):
        return fixture.archive(self.root, name, value)

    def children(self):
        result = []
        # This control injects the native-waiter boundary, never product proof.
        self.addCleanup(fixture._host_runs.clear)
        for arm in ('C1', 'C3', 'A', 'B', 'S1', 'S3', 'F1', 'F3', 'K'):
            row = dict(arm=arm, instance='drill-99-'+arm, native_rc=0, verdict='GREEN')
            fixture._host_runs[row['instance']] = copy.deepcopy(row)
            row['raw'] = self.ref(arm+'.json', row)
            result.append(row)
        return result

    def bounds(self, steps):
        refs = {}
        for i, entry in enumerate(steps):
            step = entry['step']
            row = {k: self.profile[k] for k in ('topology', 'workload_digest', 'budget_digest')}
            row.update({k: step[k] for k in ('direction', 'from', 'to')})
            row.update(step_id=step['id'], upper_ns=S, includes_prepare_stage=True,
                       artifact_digest=hashlib.sha256(fixture.canonical(self.profile['artifacts'])).hexdigest())
            refs[step['id']] = self.ref(f'upper-{i}.json', row)
        return refs

    def test_frozen_manifest_and_exact_ceiling_union(self):
        try:
            fixture.validate_manifest(self.manifest)
        except ValueError as error:
            self.fail(str(error))
        self.assertEqual(self.manifest['required_children'], ['C1', 'C3', 'A', 'B', 'S1', 'S3', 'F1', 'F3', 'K'])
        shell = (fixture.HERE.parent / '99-hot-update-fullstack.sh').read_text()
        for field in ('arms', 'fixture', 'grows', 'worst', 'forgoes'):
            lines = [r for r in shell.splitlines() if r.startswith('# '+field+':')]
            self.assertEqual(len(lines), 1)
            values = lines[0].split(':', 1)[1].split()
            self.assertEqual([v.split('=')[0] for v in values], self.manifest['required_children'])
            if field == 'worst':
                self.assertEqual([int(v.split('=')[1]) for v in values],
                                 [r['worst_s'] for r in self.manifest['unit_ceilings']])

    def test_manifest_missing_child_leg_or_mixed_direction_is_rejected(self):
        cases = []
        value = copy.deepcopy(self.manifest); value['required_children'].pop(); cases.append(value)
        value = copy.deepcopy(self.manifest); value['rows'][1]['leg'] = 'leg1'; cases.append(value)
        value = copy.deepcopy(self.manifest); value['rows'][3]['actions'][0][1] = 'forward'; cases.append(value)
        value = copy.deepcopy(self.manifest); value['infra_obligations'].pop(); cases.append(value)
        value = copy.deepcopy(self.manifest); value['arms'][2]['cohorts'].pop(); cases.append(value)
        value = copy.deepcopy(self.manifest); value['unit_ceilings'][2]['ceiling_s'] = 12001; cases.append(value)
        for value in cases:
            with self.assertRaises(ValueError): fixture.validate_manifest(value)

    def test_each_arm_keeps_its_exact_scope_and_all_runtime_hops(self):
        expected = {'C1': 0, 'C3': 0, 'A': 44, 'B': 84, 'S1': 21, 'S3': 39, 'F1': 21, 'F3': 39, 'K': 0}
        for arm, count in expected.items():
            with self.subTest(arm=arm):
                p = profile(self.root, arm)
                rows = fixture.step_manifest(self.manifest, p)
                self.assertEqual(len(rows), count)
                for row in rows:
                    if row['step']['target']['role'] in ('nats', 'caddy'):
                        self.assertEqual(row['obligations'][-7:], ['physical-cohort', 'temporary-ingress', 'helper',
                            'launcher', 'canonical-ingress', 'old-worker-exit', 'temporary-owner-exit'])
        mixed = [r['step'] for r in fixture.step_manifest(self.manifest, self.profile) if r['step']['parent_id'] == 'R4']
        self.assertEqual([r['direction'] for r in mixed], ['reverse', 'reverse'] + ['forward']*5)
        self.assertEqual(mixed[2]['depends_on'], [mixed[1]['id']])

    def test_step_manifest_refuses_missing_runtime_and_wrong_direction_or_leg(self):
        for mutation in ('missing', 'direction', 'leg', 'action', 'topology'):
            p = copy.deepcopy(self.profile)
            if mutation == 'missing': p['steps'].pop(2)
            elif mutation == 'direction': p['steps'][21]['direction'] = 'forward'
            elif mutation == 'leg': p['steps'][7]['leg'] = 'leg1'
            elif mutation == 'action': p['steps'][7]['action'] = 'upgrade'
            else: p['topology'] = 'quorum'
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                fixture.step_manifest(self.manifest, p)

    def test_parent_missing_child_never_reports_green(self):
        children = self.children()
        self.assertEqual(fixture.join_children(self.manifest, children), 0)
        self.assertEqual(fixture.join_children(self.manifest, children[:-1]), 4)
        self.assertEqual(fixture.join_children(self.manifest, []), 4)
        for i in range(9):
            self.assertEqual(fixture.join_children(self.manifest, children[:i]+children[i+1:]), 4)

    def test_parent_preserves_native_priority_and_unique_instances(self):
        children = self.children()
        for i, rc in enumerate((4, 3, 2, 1)):
            children[i].update(native_rc=rc, verdict=fixture.VERDICTS[rc])
            children[i]['raw'] = self.ref(f'child-{rc}.json', {k: v for k, v in children[i].items() if k != 'raw'})
            fixture._host_runs[children[i]['instance']] = {k: v for k, v in children[i].items() if k != 'raw'}
            self.assertEqual(fixture.join_children(self.manifest, children), rc)
        for field in ('arm', 'instance'):
            broken = copy.deepcopy(children); broken[1][field] = broken[0][field]
            with self.assertRaisesRegex(ValueError, 'duplicate'): fixture.join_children(self.manifest, broken)
        broken = copy.deepcopy(children); broken[0]['native_rc'] = 75
        with self.assertRaisesRegex(ValueError, 'native'): fixture.join_children(self.manifest, broken)

    def test_parent_raw_digest_and_native_zero_are_not_trusted(self):
        children = self.children()
        children[0]['native_rc'] = 1
        children[0]['verdict'] = 'ASSERT-FAIL'
        with self.assertRaisesRegex(ValueError, 'raw mismatch'): fixture.join_children(self.manifest, children)
        Path(children[0]['raw']['path']).write_text('{}')
        with self.assertRaisesRegex(ValueError, 'digest'): fixture.join_children(self.manifest, children)

    def test_same_id_cannot_cross_expiry_or_renew_its_deadline(self):
        value = window(leg='leg2')
        fixture.transfer_window('C0', 'leg2', **dict(originals=value['originals'], receipts=value['receipts'], cut_ns=value['at_ns'], clock_id='fixture-clock'))
        for cut in (value['originals'][0]['deadline_ns'], value['originals'][0]['deadline_ns']+S):
            try:
                fixture.transfer_window('C0', 'leg2', value['originals'], value['receipts'], cut, 'fixture-clock')
            except (fixture.AssertionFailure, fixture.Incomplete) as error:
                self.assertIsInstance(error, fixture.AssertionFailure, 'expired business ID must be ASSERT-FAIL1')
                self.assertIn('original expiry', str(error))
            else:
                self.fail('same transfer ID accepted beyond expiry')
        broken = copy.deepcopy(value); broken['receipts'][0]['deadline_ns'] += S
        with self.assertRaisesRegex(fixture.AssertionFailure, 'original expiry'):
            fixture.transfer_window('C0', 'leg2', broken['originals'], broken['receipts'], broken['at_ns'], 'fixture-clock')

    def test_each_required_transfer_leg_is_live_under_original_id(self):
        for cohort in ('C0', 'C1'):
            for leg in ('leg1', 'leg2'):
                value = window(cohort, leg)
                fixture.transfer_window(cohort, leg, value['originals'], value['receipts'], value['at_ns'], 'fixture-clock')
                for field, replacement in (('leg', 'courier'), ('role', 'metadata'), ('ack_chunks', 0), ('ack_chunks', 2000)):
                    rows = copy.deepcopy(value['receipts']); rows[0][field] = replacement
                    with self.assertRaisesRegex(fixture.Incomplete, 'required live transfer leg'):
                        fixture.transfer_window(cohort, leg, value['originals'], rows, value['at_ns'], 'fixture-clock')
                with self.assertRaisesRegex(fixture.Incomplete, 'missing required transfer leg'):
                    fixture.transfer_window(cohort, leg, value['originals'], value['receipts'][:-1], value['at_ns'], 'fixture-clock')
                rows = copy.deepcopy(value['receipts']); rows[0]['transfer_id'] = 'replacement'
                with self.assertRaisesRegex(fixture.AssertionFailure, 'replacement'):
                    fixture.transfer_window(cohort, leg, value['originals'], rows, value['at_ns'], 'fixture-clock')

    def test_finite_windows_fail_incomplete_before_business_expiry(self):
        for leg, cut in (('leg1', 920*S), ('leg2', 1910*S)):
            value = window(leg=leg)
            with self.assertRaises(fixture.Incomplete):
                fixture.transfer_window('C0', leg, value['originals'], value['receipts'], cut, 'fixture-clock')
        value = window(leg='leg2'); value['receipts'][0]['first_durable_ns'] = 1061*S
        with self.assertRaisesRegex(fixture.Incomplete, 'leg2 cut window'):
            fixture.transfer_window('C0', 'leg2', value['originals'], value['receipts'], 1070*S, 'fixture-clock')

    def test_remote_boottime_cannot_be_subtracted_from_local_schedule(self):
        value = window()
        value['receipts'][1]['clock_id'] = 'another-host'
        with self.assertRaisesRegex(fixture.Incomplete, 'cross-host'):
            fixture.transfer_window('C0', 'leg1', value['originals'], value['receipts'], value['at_ns'], 'fixture-clock')

    def test_upper_bounds_need_exact_same_profile_receipts_and_fit_840s(self):
        rows = fixture.step_manifest(self.manifest, self.profile)
        refs = self.bounds(rows)
        totals = fixture.upper_bounds(self.profile, rows, refs)
        self.assertEqual(totals, {('C0', 'leg1'): 7*S, ('C0', 'leg2'): 14*S, ('C1', 'leg1'): 7*S, ('C1', 'leg2'): 16*S})
        missing = refs.copy(); missing.pop(next(iter(missing)))
        with self.assertRaisesRegex(fixture.Incomplete, 'upper-bound receipts'): fixture.upper_bounds(self.profile, rows, missing)
        old = copy.deepcopy(self.profile); old['artifacts'][0]['binary'] = 'a'*64
        with self.assertRaisesRegex(fixture.Incomplete, 'profile mismatch'): fixture.upper_bounds(old, rows, refs)
        for i, (step_id, ref) in enumerate(refs.items()):
            value = fixture.decode(fixture.read_ref(ref)); value['upper_ns'] = 60*S
            refs[step_id] = self.ref(f'slow-{i}.json', value)
        with self.assertRaisesRegex(fixture.Incomplete, 'cannot fit'): fixture.upper_bounds(self.profile, rows, refs)

    def test_p10_image_output_is_consumed_without_renaming(self):
        rows = [a for a in self.profile['artifacts'] if a['role'] in ('agent', 'broker', 'ctl')]
        ref = self.ref('image.json', rows)
        self.assertEqual(fixture.bind_image_output(self.profile, ref), rows)
        broken = copy.deepcopy(self.profile); broken['artifacts'][0]['binary'] = 'a'*64
        with self.assertRaisesRegex(ValueError, 'P10 image'): fixture.bind_image_output(broken, ref)
        with self.assertRaisesRegex(ValueError, 'P10 image'):
            fixture.bind_image_output(self.profile, self.ref('short-image.json', rows[:-1]))

    def assembly(self):
        events = []
        work = SimpleNamespace(stop=lambda: events.append('work-stop'), join=lambda: events.append('work-join'))
        ports = SimpleNamespace(call_driver=lambda *_: self.fail('unexpected product command'), workloads=work)
        assembly = fixture.Assembly(self.profile, {}, ports, self.manifest)
        return assembly, ports, events

    def test_cleanup_joins_all_owners_and_closes_control_last(self):
        assembly, ports, events = self.assembly()
        assembly.phase, assembly.work_started = 'running', True
        assembly.flow = SimpleNamespace(close=lambda: events.append('flow-close-join'))
        assembly.collector = SimpleNamespace(close=lambda: events.append('collector-close'), join=lambda: events.append('collector-join'))
        assembly.endpoints = [SimpleNamespace(close=lambda: events.append('endpoint-close'))]
        assembly.controls = [SimpleNamespace(close=lambda: events.append('control-close'))]
        assembly.stop(); assembly.join(); assembly.stop(); assembly.join()
        self.assertEqual(events, ['work-stop', 'flow-close-join', 'collector-close', 'work-join', 'collector-join', 'endpoint-close', 'control-close'])

    def test_cleanup_failure_remains_sticky_after_every_other_join(self):
        assembly, ports, events = self.assembly()
        assembly.phase, assembly.work_started = 'running', True
        def failed():
            events.append('failed-flow-join')
            raise ValueError('native supervisor rc125')
        assembly.flow = SimpleNamespace(close=failed)
        assembly.controls = [SimpleNamespace(close=lambda: events.append('control-close'))]
        assembly.stop()
        for _ in range(2):
            with self.assertRaisesRegex(ValueError, 'native supervisor join failed'): assembly.join()
        self.assertEqual(events, ['work-stop', 'failed-flow-join', 'work-join', 'control-close'])
        self.assertEqual(assembly.errors, ['flow_close_join'])

    def test_drain_requires_every_step_and_owner_join_before_two_barriers(self):
        assembly, ports, events = self.assembly()
        assembly.phase = 'running'
        ports.workloads.drain = lambda _: events.append('owners-joined') or {'complete': True}
        assembly.flow = SimpleNamespace(drain=lambda: events.append('flow-drained'))
        assembly.collector = SimpleNamespace(seal=lambda _: events.append('two-fresh-barriers') or {'complete': True})
        ports.finish = lambda *_: events.append('independent-judges') or dict(rc=0, verdict='GREEN', failures=[], missing=[])
        with self.assertRaisesRegex(fixture.Incomplete, 'required component step'): assembly.drain('early')
        self.assertEqual(events, [])
        assembly.completed = {r['step']['id'] for r in assembly.steps}
        ref = assembly.drain('fresh')
        self.assertEqual(events, ['owners-joined', 'flow-drained', 'two-fresh-barriers', 'independent-judges'])
        self.assertEqual(fixture.decode(fixture.read_ref(ref))['result']['rc'], 0)

    def test_drain_cannot_promote_a_rejected_judge(self):
        assembly, ports, _ = self.assembly()
        assembly.phase = 'running'; assembly.completed = {r['step']['id'] for r in assembly.steps}
        ports.workloads.drain = lambda _: {}
        assembly.flow = SimpleNamespace(drain=lambda: None)
        assembly.collector = SimpleNamespace(seal=lambda _: {})
        ports.finish = lambda *_: dict(rc=1, verdict='ASSERT-FAIL', failures=['missing bytes'], missing=[])
        with self.assertRaisesRegex(fixture.AssertionFailure, 'judge rejected'): assembly.drain('nonce')

    def test_lost_reply_keeps_unsafe_step_reserved_and_dependencies_block(self):
        assembly, ports, _ = self.assembly()
        assembly.phase = 'running'
        ports.before_step = lambda *_: window()
        calls = []
        def lost(method, payload):
            calls.append(method)
            raise OSError('reply lost')
        assembly.driver = fixture.ProductCLIDriver(lost)
        step = assembly.steps[0]['step']
        with self.assertRaisesRegex(OSError, 'reply lost'): assembly.apply(step['id'])
        with self.assertRaisesRegex(ValueError, 'cut repeated'): assembly.apply(step['id'])
        self.assertEqual(calls, ['stage'])
        broker = next(r['step'] for r in assembly.steps if r['step']['target']['role'] == 'broker')
        with self.assertRaisesRegex(ValueError, 'previous component'): assembly.apply(broker['id'])

    def test_original_cohort_identity_cannot_be_rewritten_between_cuts(self):
        assembly, ports, _ = self.assembly()
        assembly.phase = 'running'
        value = window(); assembly.originals['C0'] = copy.deepcopy(value['originals'])
        value['originals'][0]['deadline_ns'] += S
        ports.before_step = lambda *_: value
        with self.assertRaisesRegex(fixture.AssertionFailure, 'identity or deadline changed'):
            assembly.apply(assembly.steps[0]['step']['id'])

    def test_protocol_dispatches_lifecycle_and_driver_preserves_errors(self):
        events = []
        assembly = SimpleNamespace(start=lambda p: events.append(('start', p)) or {'ready': True},
                                   apply=lambda p: events.append(('apply', p)) or {'step': p},
                                   stop=lambda: events.append('stop'), join=lambda: events.append('join'))
        incoming = io.BytesIO(b'{"id":1,"method":"start","payload":{"scope":"hermetic"}}\n'
                              b'{"id":2,"method":"apply","payload":{"step_id":"R1-agent"}}\n'
                              b'{"id":3,"method":"stop","payload":{}}\n')
        outgoing = io.BytesIO()
        self.assertEqual(fixture.serve(assembly, incoming, outgoing), 0)
        self.assertEqual([fixture.decode(r)['rc'] for r in outgoing.getvalue().splitlines()], [0, 0, 0])
        self.assertEqual(events[:4], [('start', {'scope': 'hermetic'}), ('apply', 'R1-agent'), 'stop', 'join'])
        output = io.BytesIO()
        call = fixture.driver_bridge(io.BytesIO(b'{"view":null,"error":"native failure"}\n'), output)
        with self.assertRaisesRegex(ValueError, 'Go product driver failed'): call('stage', {'id': 'original'})
        self.assertEqual(fixture.decode(output.getvalue()), {'driver': 'stage', 'payload': {'id': 'original'}})

    def test_preflight_failure_does_not_open_custody_or_start_business(self):
        assembly, ports, events = self.assembly()
        ports.validate = lambda _: events.append('validate')
        assembly.inputs['image_output'] = self.ref('image.json', [a for a in self.profile['artifacts'] if a['role'] in ('agent', 'broker', 'ctl')])
        assembly.inputs['upper_bounds'] = {}
        with patch.object(fixture, 'open_custody', side_effect=AssertionError('side effect before preflight')):
            with self.assertRaisesRegex(fixture.Incomplete, 'upper-bound receipts'): assembly.start(self.profile)
        self.assertEqual(events, ['validate'])
        self.assertEqual(assembly.phase, 'new')

    def start_inputs(self):
        rows = fixture.step_manifest(self.manifest, self.profile)
        raw = b'[{"sealed":"schedule consumed through P17"}]'
        schedule_ref = self.ref('request-schedule.json', fixture.decode(raw))
        return dict(nonce='1'*32, flow_bindings={'bound': 'P11 routes'}, request_schedule=schedule_ref,
                    image_output=self.ref('image-output.json', [a for a in self.profile['artifacts'] if a['role'] in ('agent', 'broker', 'ctl')]),
                    upper_bounds=self.bounds(rows), custody={f'a{i}': dict(root=f'/host/a{i}', identity='a'*64) for i in range(4)})

    def test_start_binds_p11_peer_and_custody_interfaces_and_partial_failure(self):
        inputs = self.start_inputs()
        for fail in (False, True):
            with self.subTest(partial=fail):
                p = copy.deepcopy(self.profile); p['evidence_root'] = str(self.root / ('partial' if fail else 'normal'))
                events, sockets = [], []
                def custody(root, runtime):
                    self.assertEqual(root, '/host/'+runtime)
                    events.append('custody-'+runtime)
                    pair = tuple(SimpleNamespace(close=lambda name=name: events.append(name))
                                 for name in ('endpoint-'+runtime, 'control-'+runtime))
                    sockets.append(pair)
                    return pair
                def start_peer(source, role):
                    self.fail('FlowCohort owns actual peer launches; assembly only binds the P11b port')
                work = SimpleNamespace(start=lambda *args: events.append('work-start') or {'owner': 'hermetic'},
                                       sample=lambda *_: {}, drain=lambda *_: {},
                                       stop=lambda: events.append('work-stop'), join=lambda: events.append('work-join'))
                ports = SimpleNamespace(validate=lambda _: events.append('Go-plan'), preflight=lambda *_: events.append('preflight'),
                                        before_step=lambda *_: {}, after_step=lambda *_: {}, finish=lambda *_: {},
                                        call_driver=lambda *_: None, start_peer=start_peer, logs=object(), workloads=work)
                class Flow:
                    digest = 'c'*64
                    def __init__(self, root, nonce, sources, launch):
                        self_test.assertEqual(root, Path(p['evidence_root'])/'original-flows')
                        self_test.assertEqual((nonce, sources, launch), ('1'*32, [{'original': True}], start_peer))
                    def start(self):
                        events.append('flow-start')
                        if fail: raise ValueError('partial peer ready failure')
                    def close(self): events.append('flow-close-join')
                class Collector:
                    def __init__(self, sources, logs):
                        self_test.assertEqual(set(sources), {'a0', 'a1', 'a2', 'a3'})
                        self_test.assertIs(logs, ports.logs)
                        for (endpoint, identity), pair in zip(sources.values(), sockets):
                            self_test.assertIs(endpoint, pair[0]); self_test.assertEqual(identity, bytes.fromhex('a'*64))
                    def close(self): events.append('collector-close')
                    def join(self): events.append('collector-join')
                self_test = self
                schedule = SimpleNamespace(slots=[SimpleNamespace(scheduled_ns=p['start_ns']),
                                                  SimpleNamespace(scheduled_ns=p['end_ns']-100_000_000)])
                with patch.object(fixture, 'open_custody', side_effect=custody), patch.object(fixture, 'Collector', Collector), \
                        patch.object(fixture.flows, 'FlowCohort', Flow), patch.object(fixture.flows, 'original_sources', return_value=[{'original': True}]) as sources, \
                        patch.object(fixture, 'load_schedule', return_value=schedule):
                    assembly = fixture.Assembly(p, inputs, ports, self.manifest)
                    join_owner = work.join
                    work.join = None
                    with self.assertRaisesRegex(fixture.Incomplete, 'workload and observation bindings'): assembly.start(p)
                    self.assertEqual(events, ['Go-plan'])
                    events.clear(); work.join = join_owner
                    if fail:
                        with self.assertRaisesRegex(ValueError, 'partial peer'): assembly.start(p)
                        self.assertNotIn('work-start', events)
                    else:
                        fixture.read_ref(assembly.start(p)); assembly.stop(); assembly.join()
                    self.assertEqual(events[:2], ['Go-plan', 'preflight'])
                    self.assertEqual(sources.call_args.args[1:], ('1'*32, 12000, inputs['flow_bindings']))
                    self.assertLess(events.index('collector-join'), events.index('control-a0'))
                    self.assertEqual(events[-4:], ['control-a0', 'control-a1', 'control-a2', 'control-a3'])

    def test_step_completion_needs_obligations_and_p10_execution_join(self):
        assembly, ports, _ = self.assembly()
        assembly.phase = 'running'
        value = window()
        ports.before_step = lambda *_: copy.deepcopy(value)
        calls = []
        step = assembly.steps[0]['step']
        def call(method, payload):
            calls.append(method)
            view = {k: {} for k in ('stamp', 'historical', 'current', 'phase', 'outcome', 'code', 'detail',
                                   'deferred', 'control', 'resources', 'released', 'raw')}
            stamp = dict(tx_id='original-transaction', cut_id='this-cut', process={'key': step['target']},
                         direction=step['direction'], generation=2, scope_digest='a'*64,
                         query_nonce=payload.get('nonce', 'not-the-query'))
            view.update(stamp=stamp, phase='completed', outcome='success', deferred=[])
            for name, kind in zip(('control', 'resources', 'released'),
                                  ('control_ready', 'resources_migrated', 'old_generation_released')):
                view[name] = dict(kind=kind, known=True, complete=True, stamp=stamp)
            return fixture.canonical(view)
        assembly.driver = fixture.ProductCLIDriver(call)
        raw = self.ref('obligation.json', {'test': 'hermetic protocol only'})
        receipt = dict(obligations={}, window=value, kernel_raw=raw)
        ports.complete_step = lambda *args: calls.append('native-completion')
        ports.validate_obligation = lambda step, kind, native, current: self.assertEqual(native, {'test': 'hermetic protocol only'})
        def after(step, obligations, staged, applied, current):
            if receipt['obligations'] == {'missing': True}:
                return dict(receipt, obligations={})
            refs = {}
            for kind in obligations:
                proof = dict(schema='fullstack-step-obligation-v1', kind=kind, step=step, stamp=current['stamp'],
                             known=True, complete=True, evidence=raw)
                refs[kind] = self.ref(kind+'.'+str(len(calls))+'.json', proof)
                refs[kind]['schema'] = proof['schema']
            receipt['obligations'] = refs
            return copy.deepcopy(receipt)
        ports.after_step = after
        supplier = SimpleNamespace(join_execution=lambda *args: {'joined': args[0]['id']})
        with patch.object(fixture, 'artifact_supplier', return_value=supplier):
            result = fixture.decode(fixture.read_ref(assembly.apply(assembly.steps[0]['step']['id'])))
        self.assertEqual(calls, ['stage', 'apply', 'query', 'native-completion'])
        self.assertEqual(result['execution_join'], {'joined': 'T1-agent'})
        self.assertEqual(assembly.completed, {assembly.steps[0]['step']['id']})
        receipt['obligations'].pop('old-released')
        receipt['obligations'] = {'missing': True}
        with patch.object(fixture, 'artifact_supplier', return_value=supplier), self.assertRaisesRegex(ValueError, 'retirement obligation missing'):
            assembly.apply(assembly.steps[1]['step']['id'])

    # origin: S10 internal review rv3-assembly F3
    def test_current_completion_rejects_foreign_or_untyped_obligations(self):
        step = self.profile['steps'][0]
        stamp = dict(process={'key': step['target']}, direction=step['direction'], query_nonce='fresh',
                     tx_id='current-tx', cut_id='current-cut', generation=2, scope_digest='a'*64)
        current = dict(phase='completed', outcome='success', deferred=[], stamp=stamp)
        for name, kind in zip(('control', 'resources', 'released'),
                              ('control_ready', 'resources_migrated', 'old_generation_released')):
            current[name] = dict(kind=kind, known=True, complete=True, stamp=stamp)
        native = self.ref('native-obligation.json', {'native': 'decoded'})
        proof = dict(schema='fullstack-step-obligation-v1', kind='old-released', step=step, stamp=stamp,
                     known=True, complete=True, evidence=native)
        ports = SimpleNamespace(complete_step=lambda *args: None,
                                validate_obligation=lambda step, kind, raw, view: self.assertEqual(raw, {'native': 'decoded'}))
        valid = self.ref('typed-obligation.json', proof); valid['schema'] = proof['schema']
        fixture.complete_step(ports, step, current, 'fresh', {'old-released': valid})
        for defect in ('unrelated', 'foreign-cut', 'foreign-step', 'wrong-kind', 'incomplete'):
            bad = copy.deepcopy(proof)
            if defect == 'unrelated': bad = {'work_performed': False}
            elif defect == 'foreign-cut': bad['stamp']['cut_id'] = 'older-cut'
            elif defect == 'foreign-step': bad['step']['id'] = 'another-step'
            elif defect == 'wrong-kind': bad['kind'] = 'prepare'
            else: bad['complete'] = False
            ref = self.ref(defect+'.json', bad); ref['schema'] = proof['schema']
            with self.subTest(defect=defect), self.assertRaises(ValueError):
                fixture.complete_step(ports, step, current, 'fresh', {'old-released': ref})
        with self.assertRaisesRegex(ValueError, 'identity'):
            fixture.complete_step(ports, step, current, 'replayed', {'old-released': valid})

    def test_relay_never_reuses_c0_id_with_a_new_clock(self):
        assembly, ports, _ = self.assembly()
        assembly.phase = 'running'
        assembly.originals['C0'] = window()['originals']
        value = window('C1')
        for original, old in zip(value['originals'], assembly.originals['C0']):
            original['transfer_id'] = old['transfer_id']
        ports.before_step = lambda *_: value
        step = next(r['step'] for r in assembly.steps if r['step']['parent_id'] == 'R4')
        assembly.completed = set(step['depends_on'])
        with self.assertRaisesRegex(fixture.AssertionFailure, 'relay reused an original ID'): assembly.apply(step['id'])

    def test_host_exit_zero_requires_independent_judge_and_supervisor(self):
        self.native_host_control()

    def native_host_control(self, defect=None):
        p = copy.deepcopy(self.profile); p['tier'] = 'deploy'
        p['evidence_root'] = str(self.root / ('run-'+str(defect)))
        p['command_bindings'] = {}
        snapshot = dict(source_commit='candidate', tree_digest='a'*64, dirty_digest='b'*64)
        for artifact in p['artifacts']:
            artifact.update(source_commit='candidate', tree='a'*64, dirty='b'*64)
        image = self.ref('image-'+str(defect)+'.json', [a for a in p['artifacts'] if a['role'] in ('agent', 'broker', 'ctl')])
        (self.root / 'inputs.json').write_bytes(fixture.canonical({'image_output': image}))
        profile_path = self.root / ('profile-'+str(defect)+'.json'); profile_path.write_bytes(fixture.canonical(p))
        binary = str(Path(sys.executable).resolve())  # The independent source boundary is injected below.
        raw = self.ref('hermetic-input-'+str(defect)+'.json', {'protocol': True})
        calls = []
        class Child:
            def __init__(self, argv, **kwargs):
                calls.append(argv)
                self.judge = 'judge' in argv
                if self.judge:
                    result = dict(rc=1, verdict='ASSERT-FAIL', failures=['control'], missing=[], inputs=[raw])
                    if defect == 'short-result': result = dict(rc=1, verdict='ASSERT-FAIL', inputs=[raw])
                    if defect == 'wrong-inputs': result['inputs'] = []
                    if defect == 'missing-product': result.update(rc=0, verdict='GREEN', failures=[])
                    kwargs['stdout'].write(fixture.canonical(result))
                else:
                    (Path(p['evidence_root'])/'evidence.json').write_bytes(fixture.canonical({'profile': p, 'raw': [raw], 'samples': []}))
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def wait(self): return (0 if defect == 'missing-product' else 1) if self.judge else 0
        supplier = SimpleNamespace(source_snapshot=lambda _: (snapshot, b''))
        self.addCleanup(fixture._host_runs.clear)
        with patch.object(fixture.flows, 'source_pinned_tools', return_value=([binary]*3, [])), \
                patch.object(fixture, 'artifact_supplier', return_value=supplier), patch.object(fixture.subprocess, 'Popen', Child):
            rc = fixture.run_host('A', p['evidence_root'], profile_path, binary, binary, binary)
            self.assertEqual(rc, 1)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(argv[:2] == [binary, '--'] for argv in calls))
        self.assertEqual(fixture.decode((Path(p['evidence_root'])/'fullstack-host-status.json').read_bytes()), {'native_rc': 0})
        self.assertEqual(len(fixture._host_runs), 1)

    # origin: S10 internal review rv3-assembly F1
    def test_result_cannot_replace_full_judge_or_product_execution(self):
        for defect in ('short-result', 'wrong-inputs', 'missing-product'):
            with self.subTest(defect=defect), self.assertRaises(ValueError):
                self.native_host_control(defect)

    # origin: S10 internal review rv3-assembly F1
    def test_archived_native_summaries_have_no_waiter_authority(self):
        rows = self.children()
        fixture._host_runs.clear()
        with self.assertRaisesRegex(fixture.Incomplete, 'original runner'):
            fixture.join_children(self.manifest, rows)

    def test_missing_deployment_artifact_is_pending_before_launch(self):
        p = copy.deepcopy(self.profile); p['tier'] = 'deploy'
        profile_path = self.root / 'profile.json'; profile_path.write_bytes(fixture.canonical(p))
        with patch.object(fixture.subprocess, 'Popen', side_effect=AssertionError('missing prerequisite launched')):
            with self.assertRaisesRegex(fixture.Incomplete, 'NOT_RUN pending'):
                fixture.run_host('A', self.root, profile_path, '', '', '')

    def entry(self, *argv):
        stderr = io.StringIO()
        with patch.object(sys, 'argv', ['fullstack_fixture.py', *argv]), contextlib.redirect_stderr(stderr):
            return fixture.main(), stderr.getvalue()

    # Drill 99 maps native 1 to ASSERT-FAIL. A sealed profile missing a member
    # raised KeyError straight through main(), and Python's uncaught-exception
    # status 1 then read as a judged assertion instead of SETUP-RED.
    # origin: hot-update S10 plan §9 / L9; r9d REPORT Request 4
    def test_entry_maps_malformed_evidence_and_crashes_to_setup_red(self):
        run = ('run', '--arm', 'A', '--root', str(self.root / 'run'), '--profile', str(self.root / 'profile.json'))
        for name, document in (('KeyError', {}), ('TypeError', []), ('KeyError', dict(tier='deploy', id=fixture.PREFIX + 'A'))):
            with self.subTest(error=name, document=document), \
                    patch.object(fixture.flows, 'source_pinned_tools', return_value=(['/h', '/s', '/j'], [])):
                (self.root / 'profile.json').write_bytes(fixture.canonical(document))
                self.assertEqual((2, 'fullstack assembly setup evidence invalid\n'), self.entry(*run))
        with patch.object(fixture, 'load_manifest', side_effect=ZeroDivisionError):
            rc, stderr = self.entry('join', '--receipts', str(self.root / 'absent.json'))
        self.assertEqual((2, 'fullstack assembly SETUP-RED: unclassified driver failure: ZeroDivisionError\n'), (rc, stderr))
        # Native statuses from the original waiters still pass through unchanged.
        for native in (0, 1, 3, 4):
            with self.subTest(native=native), patch.object(fixture, 'run_host', return_value=native):
                self.assertEqual((native, ''), self.entry(*run))
        with patch.object(fixture, 'run_host', side_effect=fixture.Incomplete('NOT_RUN pending P25 host')):
            self.assertEqual((4, 'NOT_RUN pending P25 host\n'), self.entry(*run))

    # A helper that fails to import (e.g. a merge conflict left in it) aborts
    # before main() runs; the drill entry must still exit SETUP 2, not 1.
    # The copy runs in-process as __main__; importers keep the real error.
    # origin: hot-update S10 plan §9 / L9; r9d REPORT Request 4
    def test_entry_maps_a_broken_helper_import_to_setup_red(self):
        broken = self.root / 'broken-lib'
        broken.mkdir()
        (broken / 'fullstack_fixture.py').write_bytes((fixture.HERE / 'fullstack_fixture.py').read_bytes())
        (broken / 'fullstack_flow.py').write_text("raise RuntimeError('<<<<<<< merge conflict left in a helper')\n")
        saved_path, saved_flow = list(sys.path), sys.modules.pop('fullstack_flow')
        try:
            stderr = io.StringIO()
            with patch.object(sys, 'argv', ['fullstack_fixture.py', 'join', '--receipts', 'absent']), \
                    contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as status:
                runpy.run_path(str(broken / 'fullstack_fixture.py'), run_name='__main__')
            self.assertEqual((2, 'fullstack assembly SETUP-RED: helper import failed: RuntimeError\n'),
                             (status.exception.code, stderr.getvalue()))
            sys.modules.pop('fullstack_flow', None)
            with self.assertRaisesRegex(RuntimeError, 'merge conflict'):
                runpy.run_path(str(broken / 'fullstack_fixture.py'), run_name='fullstack_fixture_importer')
        finally:
            sys.path[:] = saved_path
            sys.modules['fullstack_flow'] = saved_flow


if __name__ == '__main__':
    unittest.main()
