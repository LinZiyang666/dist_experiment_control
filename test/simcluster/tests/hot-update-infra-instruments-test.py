#!/usr/bin/env -S python3 -I -B
"""Hermetic live-supply admission and S7 consumer compatibility controls.

No deployment command runs here. Real workspace-built binaries are pinned as
files; protocol inputs remain explicitly manufactured reader controls.
"""
import copy
import importlib.util
import json
import os
from pathlib import Path
import socket
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SIM = Path(__file__).resolve().parents[1]
LIB = SIM / 'drills/lib'
sys.path.insert(0, str(LIB))
import transfer_fixture as s7


def module(name):
    spec = importlib.util.spec_from_file_location(name, LIB / (name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def manifest(root, seconds=900):
    """Public reader input only; this fixture is never launched as business."""
    processes, transfers = [], []
    for i, name in enumerate(s7.COHORTS):
        label, ctl = 'agent' + str(i), 'ctl' + str(i)
        processes.append(dict(label=label, role='agent', artifact_shas=['a' * 64],
            home=str(root / label), binary_path=str(root / ('agt' + str(i % 2)) / 'tether')))
        processes.append(dict(label=ctl, role='ctl', artifact_shas=['a' * 64]))
        size = 66549440 if name.startswith('small-') else 1 << 30
        rate = (size + 1023) // 1024
        transfers.append(dict(cohort=name, transfer_id='transfer' + str(i), entry_nonce='entry' + str(i),
            bucket='bucket' + str(i), session_id='session' + str(i), object_version='NUID' + str(i),
            object_name='object' + str(i), target=str(root / ('target' + str(i))), size=size,
            sha256='b' * 64, rate_limit=rate, agent_producer=label, ctl_producer=ctl,
            object_stream='OBJ_bucket' + str(i), history_stream='history' + str(i),
            history_subject='history.transfer' + str(i),
            resource_refs={kind: kind + str(i) for kind in ('source', 'tmp', 'parent')},
            required_resources={kind + str(i): ['infra-custody'] for kind in ('source', 'tmp', 'parent')}))
    return dict(schema=1, mode='live', consumer='infra-transfer-v1', quiet_seconds=2, processes=processes,
        transfers=transfers, cuts=[], nats=dict(url='nats://127.0.0.1:9'),
        control=dict(launch=dict(argv=['/unlaunched/control', '{logical_id}', '{effect}'], home=str(root / 'control'))),
        infra=dict(schema='infra-transfer-profile-v1', component='nats', topology='quorum',
            profile='adjacent-cycle', seconds=seconds, rates={t['cohort']: t['rate_limit'] for t in transfers}))


class InfraTransferAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='infra-transfer-admission-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.path = self.root / 'manifest.json'
        self.value = manifest(self.root)

    def read(self):
        self.path.write_text(json.dumps(self.value))
        return s7.read_manifest(self.path)

    # origin: S9 internal review s9-instruments F1; S7 frozen plan §10.3
    def test_infra_consumer_accepts_fixed_product_rate_and_shared_agent_binaries(self):
        actual = self.read()
        self.assertEqual(actual['infra']['seconds'], 900)
        self.assertEqual(actual['transfers'], self.value['transfers'])
        self.assertEqual(len({p['home'] for p in actual['processes'] if p['role'] == 'agent'}), 4)
        self.assertEqual(len({p['binary_path'] for p in actual['processes'] if p['role'] == 'agent'}), 2)

    # origin: S9 internal review s9-instruments F1; S7 frozen plan §10.3
    def test_infra_consumer_cannot_relax_the_original_s7_drills(self):
        self.value.pop('consumer')
        with self.assertRaises(Exception):
            self.read()

    # origin: S9 internal review s9-instruments F2; S7 frozen plan §1.2
    def test_infra_consumer_refuses_impossible_product_budget_before_launch(self):
        self.value['infra']['seconds'] = 1200
        for transfer in self.value['transfers']:
            transfer['rate_limit'] = transfer['size'] // 1260
            self.value['infra']['rates'][transfer['cohort']] = transfer['rate_limit']
        with self.assertRaisesRegex(Exception, 'infra_product_budget'):
            self.read()

    # origin: S9 internal review s9-instruments F1; S9 frozen plan §9.1
    def test_infra_consumer_rejects_missing_or_changed_original_admission(self):
        original = copy.deepcopy(self.value)
        for defect in ('rate', 'short', 'resources', 'cohort', 'owner', 'shared_owner', 'arm', 'consumer'):
            with self.subTest(defect=defect):
                self.value = copy.deepcopy(original)
                if defect == 'rate': self.value['transfers'][0]['rate_limit'] += 1
                if defect == 'short': self.value['infra']['seconds'] = 1024
                if defect == 'resources': self.value['transfers'][0]['required_resources'] = {}
                if defect == 'cohort': self.value['transfers'].pop()
                if defect == 'owner': self.value['transfers'][0]['agent_producer'] = 'absent'
                if defect == 'shared_owner': self.value['transfers'][0]['ctl_producer'] = 'ctl1'
                if defect == 'arm': self.value['arm'] = 'A'
                if defect == 'consumer': self.value['consumer'] = 'unknown-live-consumer'
                with self.assertRaises(Exception): self.read()

    # origin: S9 internal review s9-instruments F2; S7 frozen plan §1.2
    def test_infra_completion_uses_the_original_rate_budget(self):
        adapter = module('infra_transfer_fixture')
        actual = self.read()
        for transfer in actual['transfers']:
            self.assertEqual(adapter.completion_seconds(transfer), 2108)


class InfraTransferRuntimeTests(unittest.TestCase):
    setUp = InfraTransferAdmissionTests.setUp
    read = InfraTransferAdmissionTests.read

    # origin: S9 internal review s9-instruments F2; S9 frozen plan §9.1
    def test_infra_observer_judge_allows_the_sealed_product_budget(self):
        import hot_update_infra_workload as workload
        self.read()
        instance = object.__new__(workload.Fixture)
        instance.deployment = dict(s7=dict(manifest=str(self.path), evidence=str(self.root)))
        instance.supervisor = mock.Mock()
        instance.supervisor.run.return_value = (0, b'{"native_rc":0}')
        instance.s7('judge')
        self.assertEqual(instance.supervisor.run.call_args.args[1], 2138)
    # origin: S9 internal review s9-instruments F1; S9 frozen plan §9.1
    def test_infra_projection_keeps_original_identity_and_real_rate_argv(self):
        adapter = module('infra_transfer_fixture')
        for transfer in self.value['transfers']:
            process = next(p for p in self.value['processes'] if p['label'] == transfer['ctl_producer'])
            process['launch'] = dict(argv=['/unlaunched/tether-candidate', transfer['cohort'].split('-')[1],
                '--rate-limit', str(transfer['rate_limit'])], home=str(self.root / process['label']))
        original = copy.deepcopy(self.value)
        self.path.write_text(json.dumps(self.value))
        source_sha = adapter.hashlib.sha256(self.path.read_bytes()).hexdigest()
        context = dict(self.value['infra'], evidence=str(self.root))
        output = self.root / 'projected.json'
        adapter.project(self.path, context, output)
        actual = s7.read_manifest(output)
        self.assertEqual(actual['transfers'], original['transfers'])
        self.assertEqual(actual['processes'], original['processes'])
        self.assertEqual(adapter.hashlib.sha256(self.path.read_bytes()).hexdigest(), source_sha)
        self.value['processes'][1]['launch']['argv'][-1] = '1'
        self.path.write_text(json.dumps(self.value))
        with self.assertRaisesRegex(ValueError, 'infra_rate_changed'):
            adapter.project(self.path, context, self.root / 'conflict.json')
        self.assertFalse((self.root / 'conflict.json').exists())

    # origin: S9 internal review s9-instruments F2; S7 frozen plan §10.3
    def test_infra_templates_freeze_each_arm_rate_before_original_launch(self):
        adapter = module('infra_transfer_fixture')
        self.value['schema'] = 'infra-transfer-template-v1'
        for transfer in self.value['transfers']:
            transfer['rate_limit'] = '{rate_limit}'
            process = next(p for p in self.value['processes'] if p['label'] == transfer['ctl_producer'])
            process['launch'] = dict(argv=['/unlaunched/tether-candidate', transfer['cohort'].split('-')[1],
                '--rate-limit', '{rate_limit}'], home=str(self.root / process['label']))
        self.path.write_text(json.dumps(self.value))
        original = self.path.read_bytes()
        budgets = []
        for seconds in (300, 900):
            context = dict(self.value['infra'], seconds=seconds, rates=adapter.rates(seconds))
            output = self.root / ('arm-' + str(seconds) + '.json')
            adapter.project(self.path, context, output)
            actual = s7.read_manifest(output)
            for transfer in actual['transfers']:
                self.assertEqual(transfer['rate_limit'], context['rates'][transfer['cohort']])
                process = next(p for p in actual['processes'] if p['label'] == transfer['ctl_producer'])
                self.assertEqual(process['launch']['argv'][-1], str(transfer['rate_limit']))
            budgets.append(adapter.completion_seconds(actual['transfers'][0]))
        self.assertLess(budgets[0], budgets[1])
        self.assertEqual(self.path.read_bytes(), original)
        self.value['processes'][1]['launch']['argv'][-1] = '1'
        with self.assertRaisesRegex(ValueError, 'infra_template_rate'):
            adapter.freeze(self.value, context)

    # origin: S9 internal review s9-instruments F1; S7 frozen plan §10.4
    def test_infra_discovery_uses_original_custody_and_startup_digest(self):
        import transfer_keeper
        identity = transfer_keeper.process_identity(os.getpid())
        evidence = self.root / 'custody'
        evidence.mkdir()
        self.value['identity_evidence'] = str(evidence)
        for transfer in self.value['transfers']:
            transfer.update(discover_identity=True, transfer_id='', entry_nonce='', object_version='', object_name='')
        for process in self.value['processes']:
            process['artifact_shas'] = [identity['running_sha']]
        self.read()
        startup_sha = module('infra_transfer_fixture').hashlib.sha256(self.path.read_bytes()).hexdigest()
        events = []
        for i, transfer in enumerate(self.value['transfers']):
            row = dict(identity, transfer_id='real' + str(i), entry_nonce='nonce' + str(i), object_version='NUID' + str(i))
            # Prepared custody can precede the original source's NUID. The
            # later sealed observation, rather than a manifest rewrite, binds it.
            archived = dict(row, object_version='')
            (evidence / ('identity-' + transfer['cohort'] + '.json')).write_text(json.dumps(archived))
            events.append(dict(row, schema=1, producer=transfer['ctl_producer'], generation=1, cut_id='',
                kind='append', phase='sending', leg='leg1', event_seq=i + 1,
                boottime_ns=time.monotonic_ns()))
            events[-1].pop('running_sha')
        snapshot = dict(manifest_sha256=startup_sha, setup=[], events=events)
        snapshot_path = evidence / 'snapshot.json'
        snapshot_path.write_text(json.dumps(snapshot))
        resolved = s7.read_manifest(self.path)
        self.assertEqual(resolved['manifest_sha256'], startup_sha)
        self.assertEqual([t['transfer_id'] for t in resolved['transfers']], ['real' + str(i) for i in range(4)])
        self.assertEqual([t['object_version'] for t in resolved['transfers']], ['NUID' + str(i) for i in range(4)])
        snapshot['events'][0]['entry_nonce'] = 'foreign'
        snapshot_path.write_text(json.dumps(snapshot))
        with self.assertRaisesRegex(ValueError, 'infra_identity_custody'):
            s7.read_manifest(self.path)

    # origin: S9 internal review s9-instruments F1; S7 frozen plan §10.4
    def test_infra_resource_discovery_binds_sealed_kind_to_one_original_owner(self):
        import transfer_keeper
        identity = transfer_keeper.process_identity(os.getpid())
        before = len(os.listdir('/proc/self/fd'))
        instance = module('infra_transfer_fixture').fixture(self.value, self.root / 'resources')
        transfer = instance.manifest['transfers'][0]
        ref = dict(resource_id='original-random-tmp', name='tmp', kind='xfer-tmp-v1')
        metadata = dict(identity, resource=ref)
        with tempfile.TemporaryFile() as source:
            fd = os.dup(source.fileno())
            try:
                instance.keeper.accept_ofd('agent0', metadata, fd)
                self.assertEqual(len(instance.keeper.pending_ofds), 1)
                self.assertEqual(transfer['resource_refs']['tmp'], 'tmp0')
                row = dict(identity, transfer_id=transfer['transfer_id'], entry_nonce=transfer['entry_nonce'],
                    producer='agent0', generation=1, boottime_ns=time.monotonic_ns(), tmp=ref)
                instance.keeper.records.events.append(row)

                def shared_kernel_accept(owner, label, packet, descriptor):
                    self.assertEqual((label, packet['resource'], descriptor), ('agent0', ref, fd))
                    owner.witnesses[ref['resource_id']] = dict(fd=descriptor, checks=[])

                with mock.patch.object(transfer_keeper.Keeper, 'accept_ofd', shared_kernel_accept):
                    instance.keeper.resolve_ofds()
                self.assertFalse(instance.keeper.pending_ofds)
                self.assertEqual(transfer['resource_refs']['tmp'], ref['resource_id'])
                self.assertEqual(transfer['required_resources'][ref['resource_id']], ['infra-custody'])
                self.assertTrue(instance.keeper.witnesses[ref['resource_id']]['checks'][0]['equal'])
                changed = dict(ref, resource_id='replacement-random-tmp')
                instance.keeper.records.events.append(dict(row, tmp=changed))
                replacement = os.dup(source.fileno())
                def replacement_kernel_accept(owner, label, packet, descriptor):
                    owner.witnesses[packet['resource']['resource_id']] = dict(fd=descriptor, checks=[])
                with mock.patch.object(transfer_keeper.Keeper, 'accept_ofd', replacement_kernel_accept), \
                        self.assertRaisesRegex(ValueError, 'infra_resource_replaced'):
                    instance.keeper.accept_ofd('agent0', dict(metadata, resource=changed), replacement)
                with self.assertRaises(OSError):
                    os.fstat(replacement)
            finally:
                instance.close()
        self.assertEqual(len(os.listdir('/proc/self/fd')), before)

    # origin: S9 internal review s9-instruments F4; S7 frozen plan §10.4
    def test_infra_pending_ofds_close_after_the_shared_collector_joins(self):
        import transfer_keeper
        instance = module('infra_transfer_fixture').fixture(self.value, self.root / 'pending-close')
        with tempfile.TemporaryFile() as source:
            fd = os.dup(source.fileno())
            instance.keeper.pending_ofds.append(('agent0', {}, fd))
            def shared_join(owner):
                # The collector owns these entries until its original Join.
                os.fstat(fd)
            try:
                with mock.patch.object(transfer_keeper.Keeper, 'close', shared_join):
                    instance.keeper.close()
                with self.assertRaises(OSError):
                    os.fstat(fd)
            finally:
                instance.close()

    # origin: S9 internal review s9-instruments F1; S9 frozen plan §9.1
    def test_infra_reader_recovery_keeps_first_deadline_and_raw_cursors(self):
        import hot_update_infra_probe
        from transfer_observation import SetupError
        instance = module('infra_transfer_fixture').fixture(self.value, self.root / 'reader-recovery')
        start = instance.reader_attempt_ns
        budget = hot_update_infra_probe.budgets()['HOT_UPDATE_REQUEST_S'] * 1_000_000_000
        try:
            for i, scan in enumerate(instance.scans.values()):
                scan.cursor = i + 1
            expected = {name: scan.cursor for name, scan in instance.scans.items()}
            with mock.patch('time.monotonic_ns', return_value=start + budget // 2):
                instance.reader_error(ConnectionRefusedError())
            self.assertEqual(instance.fault_interval, (start, start + budget))
            self.assertEqual(instance.outage['cursors'], expected)
            instance.reader_attempt_ns = start + budget // 2
            with mock.patch('time.monotonic_ns', return_value=start + budget + 1):
                with self.assertRaisesRegex(ValueError, 'infra_reader_gap'):
                    instance.reader_error(ConnectionResetError())
            self.assertEqual(instance.fault_interval, (start, start + budget))
            self.assertIn('infra_reader_gap', instance.keeper.failures)
            instance.reader_error(SetupError('required_physical_row_missing'))
            self.assertIn('raw_scan_unavailable', instance.keeper.setup_failures)
        finally:
            instance.close()

    # origin: S9 internal review s9-instruments F2; S7 frozen plan §1.2
    def test_infra_wait_keeps_the_original_product_completion_deadline(self):
        value = self.read()
        instance = s7.Fixture(value, self.root / 'evidence')
        instance.launcher = object()  # No command runs in this clock predicate control.
        instance.started_ns = 0
        results = [{}, {t['ctl_producer']: dict(native_rc=0) for t in value['transfers']}]
        with mock.patch.object(instance, 'snapshot'), mock.patch.object(instance, 'native_results', side_effect=results), \
                mock.patch.object(instance, 'owners_joined', side_effect=[False] * 4 + [True] * 4), \
                mock.patch('transfer_fixture.time.monotonic_ns', return_value=650_000_000_000):
            instance.await_originals()
        self.assertFalse(instance.keeper.failures)
        with mock.patch.object(instance, 'snapshot'), mock.patch.object(instance, 'native_results', return_value={}), \
                mock.patch.object(instance, 'owners_joined', return_value=False), \
                mock.patch('transfer_fixture.time.monotonic_ns', return_value=2109_000_000_000):
            instance.await_originals()
        self.assertIn('original_completion_deadline', instance.keeper.failures)
        instance.keeper.close()

    # origin: S9 internal review s9-instruments F1; S7 frozen plan §10.4
    def test_infra_samples_compare_original_ofds_and_keep_a_reopen_red(self):
        import transfer_keeper
        identity = transfer_keeper.process_identity(os.getpid())
        before = len(os.listdir('/proc/self/fd'))
        for round in range(5):
            for equal in (True, False):
                with tempfile.TemporaryFile() as source, \
                        mock.patch('transfer_keeper.compare_ofd', return_value=equal) as compare:
                    instance = module('infra_transfer_fixture').fixture(self.value, self.root / ('ofd' + str(round)))
                    witness = dict(fd=os.dup(source.fileno()), product_fd=source.fileno(), checks=[],
                        pid=identity['pid'], process_start=identity['process_start'])
                    instance.keeper.witnesses['source0'] = witness
                    instance.keeper.records.events = [dict(transfer_id='transfer0', pid=identity['pid'],
                        phase='sending', kind='append', actual_ack_seq=1, generation=1)]
                    try:
                        with mock.patch.object(s7.Fixture, 'snapshot', return_value={}):
                            instance.snapshot()
                        compare.assert_called_once_with(identity['pid'], source.fileno(), witness['fd'])
                        self.assertEqual(witness['checks'][0]['equal'], equal)
                        self.assertEqual('ofd_reopened:source0' in instance.keeper.failures, not equal)
                    finally:
                        instance.close()
        self.assertEqual(len(os.listdir('/proc/self/fd')), before)

    # origin: S9 internal review s9-instruments F1; S9 composer socket custody
    def test_infra_control_preserves_long_evidence_paths_without_fd_leaks(self):
        adapter = module('infra_transfer_fixture')
        before = len(os.listdir('/proc/self/fd'))
        root = self.root / ('long-evidence-' + 'x' * 95)
        root.mkdir()
        for round in range(5):
            with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as server, \
                    socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as client:
                adapter.unix_call(server, 'bind', root)
                server.listen(1)
                adapter.unix_call(client, 'connect', root)
                with server.accept()[0] as accepted:
                    client.send(b'original-custody')
                    self.assertEqual(accepted.recv(64), b'original-custody')
            (root / 'control.sock').unlink()
        self.assertEqual(len(os.listdir('/proc/self/fd')), before)


class LiveSupplyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='infra-instrument-supply-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        spec = importlib.util.spec_from_file_location('infra_instrument_supplier', SIM / 'lib/infra-instruments.py')
        self.supplier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.supplier)
        self.contract = self.supplier.contract
        candidate = Path(os.environ['SIM_INFRA_TEST_BINARY'])
        self.assertTrue(candidate.is_absolute() and candidate.read_bytes()[:4] == b'\x7fELF')
        self.binary = self.root / 'tether-candidate'
        self.binary.write_bytes(candidate.read_bytes())
        self.binary.chmod(0o700)
        # This is schema admission only. The binary is a real workspace build;
        # the public handoff/argv below are unexecuted protocol inputs. No test
        # artifact is published as an accepted S7 handoff or deployment supply.
        self.handoff = self.root / 'reader-control.txt'
        self.handoff.write_text('HERMETIC schema control; no deployment acceptance\n')
        binary = self.port(self.binary, ['version'])
        owners = dict(prepare=binary, ready=binary, workloads={name: dict(keeper=binary, stop=binary)
            for name in ('flow', 'pty', 'exec')})
        transfer = manifest(self.root)
        transfer['schema'] = 'infra-transfer-template-v1'
        transfer['control']['launch']['argv'] = [str(self.binary), 'exec', 'agt0', '/unlaunched/effect',
                                               '{logical_id}', '{effect}']
        for process in transfer['processes']:
            process['artifact_shas'] = [self.contract.sha(self.binary)]
            process['launch'] = dict(argv=[str(self.binary), 'version'], home=str(self.root / process['label']))
            if process['role'] == 'ctl':
                row = next(t for t in transfer['transfers'] if t['ctl_producer'] == process['label'])
                row['rate_limit'] = '{rate_limit}'
                process['launch']['argv'] = [str(self.binary), row['cohort'].split('-')[1],
                    '--rate-limit', str(row['rate_limit']), '/unlaunched/source', 'node:/unlaunched/target']
        self.transfer_manifest = self.root / 'transfer.json'
        self.transfer_manifest.write_bytes(self.contract.canonical(transfer))
        env = str(Path('/usr/bin/env').resolve())
        client = self.port(env, ['TETHER_ATTEMPT_LOGICAL_ID={logical_id}', str(self.binary), 'version'],
                           [self.binary])
        client.update(mode='cli', expected_output='unused schema-only output')
        self.value = dict(schema='infra-instruments-v1', transfer_manifest=self.contract.pin(self.transfer_manifest),
            handoff=dict(schema='transfer-v1', accepted_by='lead', files={key: self.contract.pin(self.handoff)
                for key in ('executable', 'source', 'schema', 'positive', 'negative')}),
            seconds={key: 900 for key in self.contract.profile_keys()}, owners=owners,
            commands={name: copy.deepcopy(client) for name in self.contract.probe.KINDS},
            readers={name: copy.deepcopy(binary) for name in ('sample', 'progress', 'effects', 'final', 'node')},
            tls={name: dict(address='127.0.0.1' if name.endswith('4') else '::1', server_name='localhost',
                ca_file=str(self.handoff), reader=copy.deepcopy(binary)) for name in self.contract.probe.TLS_LANES},
            faults={name: {action: copy.deepcopy(binary) for action in ('install', 'status', 'heal')}
                for name in self.contract.verbs.ALL_FAULTS},
            exercises={name: copy.deepcopy(binary) for name in self.contract.verbs.ALL_EXERCISES},
            configs={name: copy.deepcopy(binary) for name in self.contract.verbs.ALL_CONFIGS})
        self.deployment = self.root / 'deployment.json'
        self.output = self.root / 'supply.json'
        self.vendor = self.root / 'vendor'

    def port(self, executable, argv, inputs=()):
        return dict(argv=[str(executable), *argv], sha256=self.contract.sha(executable),
                    inputs=[self.contract.pin(p) for p in inputs])

    def write(self):
        self.deployment.write_bytes(self.contract.canonical(self.value))

    # origin: S9 internal review s9-instruments F3; S9 frozen plan §9.4
    def test_live_instrument_supplier_has_an_executable_entry(self):
        self.assertTrue((SIM / 'lib/infra-instruments.py').is_file(), 'live instrument supplier absent')

    # origin: S9 internal review s9-instruments F3; S9 frozen plan §9.4
    def test_live_supply_has_every_arm_owner_and_pinned_instrument(self):
        self.value['seconds'][sorted(self.value['seconds'])[0]] = 300
        actual = self.supplier.assemble(self.value)
        self.assertEqual(actual['mode'], 'live')
        self.assertEqual(actual['schema'], 'infra-composer-supply-v1')
        self.assertEqual(set(actual['profiles']), self.contract.profile_keys())
        self.assertIs(self.contract.validate(actual), actual)
        self.assertGreater(actual['profiles'][sorted(self.value['seconds'])[0]]['rates']['small-push'],
                           actual['profiles'][sorted(self.value['seconds'])[1]]['rates']['small-push'])
        for row in actual['profiles'].values():
            self.assertEqual(set(row['workloads']), {'flow', 'pty', 'exec', 'transfers'})
            for transfer, rate in row['rates'].items():
                size = 66549440 if transfer.startswith('small-') else 1 << 30
                self.assertGreater(size // rate, row['seconds'])
                self.assertLessEqual(module('infra_transfer_fixture').completion_seconds(dict(size=size, rate_limit=rate)), 2108)
        for instrument in self.contract.all_specs(actual):
            self.assertEqual(set(instrument) & {'argv', 'sha256', 'inputs'}, {'argv', 'sha256', 'inputs'})

    # origin: S9 internal review s9-instruments F3; S9 frozen plan §9.4
    def test_live_supply_refuses_missing_instrument_and_stale_executable(self):
        original = copy.deepcopy(self.value)
        source_bytes = self.transfer_manifest.read_bytes()
        for defect in ('keeper', 'fault', 'reader', 'executable', 'input', 'sha', 'rate', 'artifact', 'session'):
            with self.subTest(defect=defect):
                self.transfer_manifest.write_bytes(source_bytes)
                self.value = copy.deepcopy(original)
                if defect == 'keeper': del self.value['owners']['workloads']['flow']['keeper']
                if defect == 'fault': self.value['faults'].pop(next(iter(self.value['faults'])))
                if defect == 'reader': self.value['readers'].pop('sample')
                if defect == 'executable': self.value['readers']['sample']['argv'][0] = str(self.root / 'absent')
                if defect == 'input': self.value['commands']['read']['inputs'][0]['sha256'] = 'f' * 64
                if defect == 'sha': self.value['readers']['sample']['sha256'] = 'f' * 64
                if defect == 'rate':
                    transfer = json.loads(self.transfer_manifest.read_bytes())
                    transfer['processes'][1]['launch']['argv'][3] = '1'
                    self.transfer_manifest.write_bytes(self.contract.canonical(transfer))
                    self.value['transfer_manifest'] = self.contract.pin(self.transfer_manifest)
                if defect in ('artifact', 'session'):
                    transfer = json.loads(source_bytes)
                    if defect == 'artifact': transfer['processes'][1]['artifact_shas'] = ['f' * 64]
                    if defect == 'session': transfer['transfers'][0]['session_id'] = transfer['transfers'][1]['session_id']
                    self.transfer_manifest.write_bytes(self.contract.canonical(transfer))
                    self.value['transfer_manifest'] = self.contract.pin(self.transfer_manifest)
                self.write()
                self.output.write_text('stale supply')
                self.vendor.mkdir(exist_ok=True)
                (self.vendor / 'infra-fixture.json').write_text('stale binding')
                with self.assertRaises((ValueError, OSError)):
                    self.supplier.stage(self.deployment, self.output, self.vendor)
                self.assertFalse(self.output.exists())
                self.assertFalse((self.vendor / 'infra-fixture.json').exists())

    # origin: S9 internal review s9-instruments F3; S9 frozen plan §9.4
    def test_live_supply_stages_through_the_p26_composer_hook(self):
        self.write()
        actual = self.supplier.stage(self.deployment, self.output, self.vendor)
        self.assertEqual(self.contract.load(self.output), actual)
        binding = self.contract.read(self.vendor / 'infra-fixture.json')
        self.assertEqual(binding['schema'], 'infra-binding-v1')
        self.assertEqual(binding['mode'], 'live')
        self.assertEqual(set(binding['commands']), set(self.contract.probe.KINDS))
        # The compatibility reader is in the staged source closure, so a new
        # admission rule cannot be substituted behind an old binding.
        paths = {row['path'] for row in binding['cohort']['keeper']['inputs']}
        self.assertIn(str(LIB / 'infra_transfer_fixture.py'), paths)
        self.binary.write_bytes(self.binary.read_bytes() + b'changed')
        with self.assertRaisesRegex(ValueError, 'composer_pin_bytes'):
            self.contract.load(self.output)

    # origin: S9 internal review s9-instruments F2; S7 frozen plan §1.2
    def test_live_supply_rejects_a_window_outside_product_admission(self):
        self.value['seconds'][next(iter(self.value['seconds']))] = 1200
        with self.assertRaisesRegex(ValueError, 'infra_product_budget'):
            self.supplier.assemble(self.value)


class OwnerPortTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='infra-owner-ports-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.plans, self.ports = module('infra_owner_plan'), module('infra_owner_ports')
        self.contract = self.plans.contract
        self.home = self.root / 'isolated-home'
        self.home.mkdir(mode=0o700)
        self.source = self.root / 'plan.json'
        self.input = self.root / 'public-input'
        self.input.write_text('public schema control; never executed as a product result\n')
        self.value = dict(schema='infra-owner-plan-v1', supervisor=self.contract.pin(os.environ['SIM_UPGRADE_SUPERVISOR']),
            product=dict(binary=self.contract.pin(os.environ['SIM_INFRA_TEST_BINARY']), home=str(self.home),
                nats_url='nats://127.0.0.1:9', node='agt1', read_path='/disposable/public-read',
                effect_root='/disposable/public-effects', remote_python='/usr/bin/python3'),
            flow=dict(here=str(SIM), sim=self.contract.pin(os.environ['SIM_UPGRADE_SUPERVISOR'])),
            pty=dict(remote_probe='/disposable/pty.py'), exec=dict(remote_probe='/disposable/exec.py'),
            native=dict(endpoint=str(self.root / 'absent-native.sock'),
                executable=self.contract.pin(Path('/usr/bin/python3').resolve()), inputs=[self.contract.pin(self.input)]),
            configs={name: self.contract.pin(self.input) for name in self.contract.verbs.ALL_CONFIGS},
            tls={lane: dict(address='127.0.0.1' if lane.endswith('4') else '::1', server_name='localhost',
                ca_file=str(self.input)) for lane in self.contract.probe.TLS_LANES})

    # origin: hot-update S9 plan §9.1 / §9.4; S9 INSTRUMENTS-2 per-port contract
    def test_owner_ports_close_the_instrument_graph_and_source_pins(self):
        self.source.write_bytes(self.contract.canonical(self.value))
        result = self.plans.supply(self.source)
        self.assertEqual(set(result), {'owners', 'commands', 'readers', 'tls', 'faults', 'exercises', 'configs'})
        for port in self.plans.PORTS:
            with self.subTest(port=port):
                spec = result
                for key in port.split('.'): spec = spec[key]
                self.contract.spec(spec)
                paths = {row['path'] for row in spec['inputs']}
                self.assertTrue({str(self.source), str(LIB / 'infra_owner_ports.py'), str(LIB / 'infra_native_custody.py'), str(LIB / 'infra_native_exports.py'),
                    str(LIB / 'infra_owner_workloads.py'), self.value['product']['binary']['path'],
                    self.value['supervisor']['path']} <= paths)
                self.assertIn(port, spec['argv'])
                self.assertEqual(set(spec) & {'argv', 'sha256', 'inputs'}, {'argv', 'sha256', 'inputs'})
        self.assertEqual(result['commands']['short_exec']['expected_output'], '{logical_id}')

    # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-2 missing and changed preconditions
    def test_every_owner_port_refuses_missing_and_changed_inputs(self):
        for port in self.plans.PORTS:
            with self.subTest(port=port):
                value = copy.deepcopy(self.value)
                value['product']['binary']['sha256'] = '0' * 64
                with self.assertRaisesRegex(ValueError, 'NOT_RUN ' + re_escape(port)):
                    self.plans.prerequisites(value, port)
                value['product'].pop('binary')
                with self.assertRaisesRegex(ValueError, 'NOT_RUN ' + re_escape(port)):
                    self.plans.prerequisites(value, port)
        for port in self.plans.PORTS:
            if port.startswith(('readers.', 'tls.', 'faults.', 'exercises.', 'configs.')) or port in ('owners.prepare', 'owners.ready'):
                value = copy.deepcopy(self.value)
                value.pop('native')
                with self.assertRaisesRegex(ValueError, 'independent_native_collector_required'):
                    self.plans.prerequisites(value, port)
        self.value['native']['inputs'][0]['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'missing_or_changed_pin'):
            self.plans.prerequisites(self.value, 'readers.sample')

    # origin: hot-update S9 plan §2.3 / §9.1; S9 INSTRUMENTS-2 no substituted observations
    def test_native_owner_ports_refuse_an_absent_collector_and_unknown_receipts(self):
        for port in self.plans.PORTS:
            if port.startswith(('readers.', 'tls.', 'faults.', 'exercises.', 'configs.')) or port in ('owners.prepare', 'owners.ready'):
                with self.subTest(port=port), self.assertRaisesRegex(ValueError, 'NOT_RUN ' + re_escape(port)):
                    self.ports.native(self.value, {}, port, [])
                with self.assertRaises((ValueError, KeyError, TypeError)):
                    self.ports.public(port, {'private_key': 'must-never-be-published'})

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-2 original CLI sink custody
    def test_request_ports_exec_the_pinned_product_and_keep_stable_effect_ids(self):
        for kind in self.contract.probe.KINDS:
            with self.subTest(kind=kind):
                argv = self.ports.command(self.value, kind, 'ab' * 16 + '-10')
                self.assertEqual(argv[0], self.value['product']['binary']['path'])
                self.assertEqual(argv[argv.index('--home') + 1], str(self.home))
                if kind in ('write', 'short_exec'):
                    self.assertIn('ab' * 16 + '-10', argv)
                    self.assertIn(str(LIB / 'infra_request_effect.py'), argv)
                    self.assertIn('exec', argv)
        with self.assertRaisesRegex(ValueError, 'logical_id_required'):
            self.ports.command(self.value, 'write', 'unbound')

    def worker(self, root):
        root.mkdir(mode=0o700)
        source = self.root / 'lifecycle-only-worker.py'
        source.write_text('''import os,sys,time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from infra_owner_lifecycle import Job, Lease, signals
root = Path(sys.argv[2]); signals()
script = "import pathlib,sys,time; p=pathlib.Path(sys.argv[1]);\\nwhile not p.exists(): time.sleep(.01)"
job = Job(root, [sys.executable, '-c', script, str(root/'finish')], os.environ['SIM_UPGRADE_SUPERVISOR'], 0)
(root/'target.pid').write_text(str(job.child.target_pid))
def drain():
    (root/'finish').touch(); job.join()
Lease(root, 'a'*64, 1).serve(lambda: None, drain, job.close, job.intact)
''')
        from flow_fixture_peer import start_supervised
        child = start_supervised([str(Path('/usr/bin/python3').resolve()), '-I', '-B', str(source), str(LIB), str(root)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(lambda: child.poll() is None and (child.kill(), child.wait(timeout=10)))
        deadline = time.monotonic() + 5
        while not (root / 'lease.json').exists():
            self.assertIsNone(child.poll())
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.01)
        state = self.contract.read(root / 'lease.json')
        return child, state

    # origin: hot-update S9 plan §9.1 / L22; S9 INSTRUMENTS-2 lifecycle only, no product doubles
    def test_owner_keeper_stop_joins_original_shared_waiters_in_five_rounds(self):
        lifecycle = module('infra_owner_lifecycle')
        before = len(os.listdir('/proc/self/fd'))
        for round in range(5):
            root = self.root / ('long-owner-evidence-' + 'x' * 96 + str(round))
            child, state = self.worker(root)
            with self.assertRaisesRegex(ValueError, 'stale_control'):
                lifecycle.request(root, 'b' * 64, 'stop')
            lifecycle.request(root, 'a' * 64, 'drain')
            self.assertIsNone(child.poll())
            lifecycle.request(root, 'a' * 64, 'stop')
            self.assertEqual(child.wait(timeout=10), 0)
            self.assertFalse((root / 'owner.sock').exists())
            self.assertEqual(self.contract.read(root / 'wait.json')['native_rc'], 0)
            self.assertFalse(Path('/proc/' + str(state['pid'])).exists())
            self.assertFalse(Path('/proc/' + (root / 'target.pid').read_text()).exists())
        self.assertEqual(len(os.listdir('/proc/self/fd')), before)

    # origin: hot-update S9 plan §9.5 / L22; S9 INSTRUMENTS-2 lifecycle only, no product doubles
    def test_owner_keeper_death_reaps_descendants_without_success(self):
        for round in range(5):
            root = self.root / ('death-' + str(round))
            child, state = self.worker(root)
            os.kill(state['pid'], signal.SIGKILL)
            self.assertNotEqual(child.wait(timeout=15), 0)
            self.assertFalse(Path('/proc/' + str(state['pid'])).exists())
            self.assertFalse(Path('/proc/' + (root / 'target.pid').read_text()).exists())

    # origin: hot-update S9 plan §9.5 / L22; S9 INSTRUMENTS-2 lifecycle only
    def test_owner_keeper_timeout_joins_all_children(self):
        check_keeper_timeout(self)

    # origin: hot-update S9 plan §9.5 / L22; S9 INSTRUMENTS-2 bounded lifecycle
    def test_owner_lease_expiry_is_failure_and_still_closes(self):
        lifecycle = module('infra_owner_lifecycle')
        root = self.root / 'expired'
        root.mkdir(mode=0o700)
        lease = lifecycle.Lease(root, 'a' * 64, 1)
        lease.expires = time.monotonic() - 1
        closed = []
        with self.assertRaisesRegex(ValueError, 'lease_expired'):
            lease.serve(lambda: None, lambda: None, lambda: closed.append(True), lambda: None)
        self.assertEqual(closed, [True])
        self.assertFalse((root / 'owner.sock').exists())

    # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-2 no stale discoverable supply
    def test_owner_plan_refusal_removes_previous_supply_and_binding(self):
        spec = importlib.util.spec_from_file_location('owner_supplier', SIM / 'lib/infra-instruments.py')
        supplier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(supplier)
        declaration, output, vendor = self.root / 'deployment.json', self.root / 'supply.json', self.root / 'vendor'
        declaration.write_text('{}')
        self.source.write_bytes(self.contract.canonical(self.value))
        vendor.mkdir()
        output.write_text('stale')
        (vendor / 'infra-fixture.json').write_text('stale')
        self.value['product']['binary']['sha256'] = '0' * 64
        self.source.write_bytes(self.contract.canonical(self.value))
        with self.assertRaisesRegex(ValueError, 'NOT_RUN owners.prepare'):
            supplier.stage(declaration, output, vendor, self.source)
        self.assertFalse(output.exists())
        self.assertFalse((vendor / 'infra-fixture.json').exists())


sys.path.insert(0, str(SIM / 'tests'))
from infra_owner_contract_cases import OwnerBoundaryTests, check_keeper_timeout, re_escape
from infra_native_contract_cases import NativePortTests
from infra_custody_export_cases import InfrastructureExportTests
from infra_supply_completion_cases import NativeAdmissionTests, ConfigCatalogTests, completion_cases, DeploymentProducerTests, FaultProducerTests, FinalProducerTests
from infra_s7_lane_cases import S7LaneTests
CompletedSupplyTests = completion_cases(LiveSupplyTests, OwnerPortTests)

if __name__ == '__main__':
    unittest.main()
