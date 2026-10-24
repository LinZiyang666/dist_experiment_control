"""Pure owner boundary and target-ledger controls; no product is simulated.

The peer control instantiates the real configure path without a connect. No
connect, docker transport, SSH command or product process is started.
origin: hot-update S9 plan §9.1 / §9.4; S9 INSTRUMENTS-2.
"""
import copy
import importlib.util
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest

LIB = Path(__file__).resolve().parents[1] / 'drills/lib'
sys.path.insert(0, str(LIB))
import infra_owner_ports as ports
import infra_owner_plan as plans
import infra_owner_lifecycle as lifecycle
import infra_request_effect as effects


def module(name):
    spec = importlib.util.spec_from_file_location(name, LIB / (name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def check_keeper_timeout(case):
    for round in range(5):
        root = case.root / ('timeout-' + str(round))
        child, state = case.worker(root)
        child.terminate()  # The observer's timeout uses this same cancellation.
        case.assertNotEqual(child.wait(timeout=15), 0)
        receipt = plans.contract.read(root / 'wait.json')
        case.assertIs(receipt['emergency'], True)
        case.assertIs(receipt['joined'], True)
        case.assertNotEqual(receipt['native_rc'], 0)
        case.assertFalse(Path('/proc/' + str(state['pid'])).exists())
        case.assertFalse(Path('/proc/' + (root / 'target.pid').read_text()).exists())


class OwnerBoundaryTests(unittest.TestCase):
    # origin: hot-update S9 plan §9.1 / §9.4; S9 INSTRUMENTS-2 complete port set
    def test_owner_port_catalog_matches_the_frozen_composer_contract(self):
        expected = {'owners.prepare', 'owners.ready'}
        expected |= {'owners.workloads.' + owner + '.' + action for owner in ('flow', 'pty', 'exec') for action in ('keeper', 'stop')}
        expected |= {'commands.' + name for name in plans.contract.probe.KINDS}
        expected |= {'readers.' + name for name in ('sample', 'progress', 'effects', 'final', 'node')}
        expected |= {'tls.' + name + '.reader' for name in plans.contract.probe.TLS_LANES}
        expected |= {'faults.' + name + '.' + action for name in plans.contract.verbs.ALL_FAULTS for action in ('install', 'status', 'heal')}
        expected |= {'exercises.' + name for name in plans.contract.verbs.ALL_EXERCISES}
        expected |= {'configs.' + name for name in plans.contract.verbs.ALL_CONFIGS}
        self.assertEqual(set(plans.PORTS), expected)
        self.assertEqual(len(plans.PORTS), len(expected))

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-2 target effect accounting
    def test_target_effects_preserve_duplicate_invocations_for_the_oracle(self):
        with tempfile.TemporaryDirectory(prefix='infra-target-effects-') as temporary:
            root, logical = Path(temporary), 'ab' * 16 + '-1'
            before = len(os.listdir('/proc/self/fd'))
            for round in range(5):
                effects.effect(root, logical, 'write')
            rows = [ports.workload.decode(line) for line in (root / 'effects.jsonl').read_bytes().splitlines()]
            self.assertEqual(rows, [dict(logical_id=logical, effect='applied')] * 5)
            self.assertEqual(ports.public('readers.effects', rows), rows)
            self.assertEqual((root / 'effects.jsonl').stat().st_mode & 0o777, 0o600)
            with self.assertRaises(ValueError): effects.effect(root, 'unbound', 'write')
            self.assertEqual(len(os.listdir('/proc/self/fd')), before)

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-2 finite source admission
    def test_flow_peer_seals_mixed_duration_sources_once_before_any_connect(self):
        from fullstack_flow import declare_source
        with tempfile.TemporaryDirectory(prefix='infra-flow-config-') as temporary:
            peer, nonce = module('infra_flow_peer').implementation(), 'ab' * 16
            source = declare_source(peer, 'tcp-0', nonce, 901)
            idle = dict(id='tcp-1', protocol='raw', **{'class': 'idle'}, size=128,
                up=peer.declaration('tcp-1.up', nonce, 0, 128), down=peer.declaration('tcp-1.down', nonce, 0, 128))
            half = dict(id='half-close', protocol='raw', **{'class': 'half-close'}, size=32768,
                up=peer.declaration('half-close.up', nonce, 20, 32768),
                down=peer.declaration('half-close.down', nonce, 21, 32768))
            before = len(os.listdir('/proc/self/fd'))
            for round in range(5):
                owner = peer.Peer(Path(temporary) / str(round), nonce, 'client')
                rows = [source, idle, half]
                try:
                    owner.command(dict(command='configure', flows=copy.deepcopy(rows)))
                    self.assertEqual(owner.specs, {row['id']: row for row in rows})
                    self.assertGreater(owner.deadline - ports.time.monotonic(), 900)
                    with self.assertRaises(ValueError): owner.command(dict(command='configure', flows=rows))
                finally:
                    owner.server.close()
            self.assertEqual(len(os.listdir('/proc/self/fd')), before)

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-2 closed finite classes
    def test_flow_peer_rejects_changed_or_duplicate_finite_declarations(self):
        from fullstack_flow import declare_source
        with tempfile.TemporaryDirectory(prefix='infra-flow-declarations-') as temporary:
            peer, nonce = module('infra_flow_peer').implementation(), 'ab' * 16
            source = declare_source(peer, 'tcp-0', nonce, 901)
            idle = dict(id='tcp-1', protocol='raw', **{'class': 'idle'}, size=128,
                up=peer.declaration('tcp-1.up', nonce, 0, 128), down=peer.declaration('tcp-1.down', nonce, 0, 128))
            for index, defect in enumerate(('hash', 'duplicate', 'class', 'duration')):
                rows = [copy.deepcopy(source), copy.deepcopy(idle)]
                if defect == 'hash': rows[1]['up']['sha256'] = '0' * 64
                if defect == 'duplicate': rows.append(copy.deepcopy(rows[1]))
                if defect == 'class': rows[1]['class'] = 'normal'
                if defect == 'duration': rows.pop(0)
                owner = peer.Peer(Path(temporary) / str(index), nonce, 'client')
                try:
                    with self.assertRaises(ValueError): owner.command(dict(command='configure', flows=rows))
                    self.assertEqual(owner.specs, {})
                finally:
                    owner.server.close()

    # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-2 executing input admission
    def test_owner_product_refuses_installed_paths_private_urls_and_non_elf_bytes(self):
        with tempfile.TemporaryDirectory(prefix='infra-product-inputs-') as temporary:
            root = Path(temporary)
            home = root / 'home'
            home.mkdir()
            binary = root / 'tether-candidate'
            binary.write_bytes(Path(os.environ['SIM_INFRA_TEST_BINARY']).read_bytes())
            binary.chmod(0o700)
            original = dict(binary=plans.contract.pin(binary), home=str(home), nats_url='nats://127.0.0.1:9',
                node='agt1', read_path='/disposable/read', effect_root='/disposable/effects', remote_python='/usr/bin/python3')
            for defect in ('installed', 'private-home', 'home-symlink', 'credentials', 'query', 'non-elf'):
                value = copy.deepcopy(original)
                if defect == 'installed':
                    path = root / 'tether'
                    path.write_bytes(binary.read_bytes()); path.chmod(0o700)
                    value['binary'] = plans.contract.pin(path)
                if defect == 'private-home':
                    path = root / '.tether'; path.mkdir(); value['home'] = str(path)
                if defect == 'home-symlink':
                    path = root / 'link'; path.symlink_to(home); value['home'] = str(path)
                if defect == 'credentials': value['nats_url'] = 'nats://user:secret@127.0.0.1:9'
                if defect == 'query': value['nats_url'] += '?token=secret'
                if defect == 'non-elf':
                    binary.write_text('#!/bin/sh\nexit 0\n'); value['binary'] = plans.contract.pin(binary)
                with self.subTest(defect=defect), self.assertRaisesRegex(ValueError, 'NOT_RUN commands.read'):
                    plans.product({'product': value}, 'commands.read')

    # origin: hot-update S9 plan §9.5 / L22; S9 INSTRUMENTS-2 fixed status, not fake success
    def test_joined_nonzero_native_status_cannot_be_normalized_to_success(self):
        with tempfile.TemporaryDirectory(prefix='infra-native-status-') as temporary:
            root = Path(temporary)
            job = lifecycle.Job(root, [str(Path('/usr/bin/python3').resolve()), '-c', 'raise SystemExit(23)'],
                                os.environ['SIM_UPGRADE_SUPERVISOR'], 0)
            with self.assertRaisesRegex(ValueError, 'original_native_status'):
                job.join()
            self.assertEqual(plans.contract.read(root / 'wait.json')['native_rc'], 23)
            job.close()

    # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-2 frozen plan pin
    def test_owner_plan_rejects_changed_source_before_reading_any_port(self):
        with tempfile.TemporaryDirectory(prefix='infra-plan-pin-') as temporary:
            source = Path(temporary) / 'plan.json'
            source.write_text('{"schema":"infra-owner-plan-v1"}\n')
            digest = plans.contract.sha(source)
            self.assertEqual(plans.load(source, digest), {'schema': 'infra-owner-plan-v1'})
            source.write_text('{"schema":"infra-owner-plan-v1","product":{}}\n')
            with self.assertRaisesRegex(ValueError, 'missing_or_changed_pin'):
                plans.load(source, digest)

    # origin: hot-update S9 plan §9.1 / LQ3; S9 INSTRUMENTS-2 startup-only supply
    def test_flow_declarations_outlast_the_whole_arm_without_pausing_sources(self):
        peer = module('flow_fixture_peer')
        from infra_owner_workloads import declare_flows
        specs = [dict(id=name, protocol='ss' if name == 'bob' else 'raw', size=128,
            **{'class': 'half-close' if name == 'half-close' else 'normal'},
            up=peer.declaration(name + '.up', 'ab' * 16, 20, 128),
            down=peer.declaration(name + '.down', 'ab' * 16, 20, 128))
            for name in ('tcp-0', 'tcp-1', 'half-close', 'ssh', 'bob')]
        declare_flows(peer, specs, 'ab' * 16, 901)
        for row in specs:
            if row['id'] == 'tcp-1':
                self.assertEqual([row[d]['records'] for d in ('up', 'down')], [0, 0])
            elif row['id'] == 'half-close':
                self.assertEqual(row['up']['records'], 20)
                self.assertEqual(row['down']['records'], (901 + 600) * 4 + 32)
            else:
                self.assertEqual(row['up']['records'], (901 + 600) * 10 + 32)
                self.assertEqual(row['supply']['rate'], 10)
                self.assertEqual(row['supply']['ceiling_seconds'], 1501)

    # origin: hot-update S9 plan §2.2 / §9.1; S9 INSTRUMENTS-2 public schema controls only
    def test_native_boundary_rejects_private_fields_inside_public_receipts(self):
        spec = importlib.util.spec_from_file_location('owner_schema_controls', Path(__file__).with_name('hot-update-infra-probe-test.py'))
        models = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(models)
        with tempfile.TemporaryDirectory(prefix='infra-public-fields-') as temporary:
            handoff = Path(temporary) / 'handoff'
            handoff.write_text('public schema control; no deployment acceptance')
            manifest, trace = models.complete_model(handoff)
            sample = copy.deepcopy(trace['samples'][5])
            sample.pop('s7')  # P32 reads S7 custody directly, outside this port.
            self.assertIs(ports.public('readers.sample', sample), sample)
            for defect in ('task', 'socket', 'process', 'prefix', 'census', 'translation', 'scalar'):
                value = copy.deepcopy(sample)
                if defect == 'task': target = value['business']['pty']['identity']
                if defect == 'socket': target = value['business']['ssh']['identity']['socket']
                if defect == 'process': target = value['current']
                if defect == 'prefix': target = value['prefix_reads'][0]['actual'][0]
                if defect == 'census': target = value['census']
                if defect == 'translation':
                    value['translations'] = {'participant': {}}
                    target = value['translations']['participant']
                if defect == 'scalar':
                    value['business']['pty']['carrier'] = {}
                    target = value['business']['pty']['carrier']
                target['private_key'] = 'must-not-cross'
                with self.subTest(defect=defect), self.assertRaises(ValueError):
                    ports.public('readers.sample', value)

    # origin: hot-update S9 plan §2.3 / L22; S9 INSTRUMENTS-2 real peer identity only
    def test_native_peer_requires_executing_sha_and_shared_supervisor_ancestry(self):
        config = dict(executable=plans.contract.pin(Path('/proc/self/exe').resolve()),
            inputs=[plans.contract.pin(Path(sys.argv[0]).resolve())])
        first, second = socket.socketpair()
        try:
            identity = ports.peer_identity(first, config, 'readers.sample')
            self.assertEqual(identity[0], os.getpid())
            ports.supervised(identity[0], plans.contract.pin(os.environ['SIM_UPGRADE_SUPERVISOR']), 'readers.sample')
            bad = copy.deepcopy(config)
            bad['executable']['sha256'] = '0' * 64
            with self.assertRaisesRegex(ValueError, 'native_peer_executable'):
                ports.peer_identity(first, bad, 'readers.sample')
            bad = copy.deepcopy(config)
            bad['inputs'] = [plans.contract.pin(Path(__file__).resolve())]
            with self.assertRaisesRegex(ValueError, 'native_peer_source'):
                ports.peer_identity(first, bad, 'readers.sample')
            with self.assertRaisesRegex(ValueError, 'shared_collector_supervisor_required'):
                ports.supervised(identity[0], dict(sha256='0' * 64), 'readers.sample')
        finally:
            first.close(); second.close()

    # origin: hot-update S9 plan §9.5 / L22; S9 INSTRUMENTS-2 lifecycle only
    def test_owner_refuses_a_different_shared_launcher_before_spawning(self):
        with tempfile.TemporaryDirectory(prefix='infra-launcher-binding-') as temporary:
            job = None
            try:
                with self.assertRaisesRegex(ValueError, 'supervisor_binding'):
                    job = lifecycle.Job(Path(temporary), [str(Path('/usr/bin/python3').resolve()), '-c', 'pass'],
                        '/absent/different-supervisor', 0)
            finally:
                if job is not None:
                    job.join()

    # origin: hot-update S9 plan §2.3 / §9.1; S9 INSTRUMENTS-2 envelope metadata only
    def test_native_response_binds_fresh_nonce_context_port_and_interval(self):
        request = dict(schema='infra-native-observation-v1', nonce='ab' * 16,
                       context_sha256='cd' * 32, port='readers.sample')
        response = dict(request, at_ns=15, payload={})
        self.assertIs(ports.bound_response(response, request, 10, 20, 'readers.sample'), response['payload'])
        for defect in ('schema', 'nonce', 'context_sha256', 'port', 'early', 'late', 'extra'):
            value = copy.deepcopy(response)
            if defect in request: value[defect] = 'unbound'
            if defect == 'early': value['at_ns'] = 9
            if defect == 'late': value['at_ns'] = 21
            if defect == 'extra': value['private_key'] = 'must-not-cross'
            with self.subTest(defect=defect), self.assertRaisesRegex(ValueError, 'native_response_binding'):
                ports.bound_response(value, request, 10, 20, 'readers.sample')

    # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-2 input custody
    def test_owner_plan_output_alias_preserves_the_plan_bytes(self):
        spec = importlib.util.spec_from_file_location('owner_alias_supplier', LIB.parents[1] / 'lib/infra-instruments.py')
        supplier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(supplier)
        with tempfile.TemporaryDirectory(prefix='infra-plan-alias-') as temporary:
            root = Path(temporary)
            source, declaration = root / 'plan.json', root / 'deployment.json'
            original = b'{"schema":"infra-owner-plan-v1"}\n'
            source.write_bytes(original)
            declaration.write_text('{}')
            with self.assertRaisesRegex(ValueError, 'instrument_input_output_alias'):
                supplier.stage(declaration, source, owner_plan=source)
            self.assertEqual(source.read_bytes(), original)


def re_escape(value):
    import re
    return re.escape(value)
