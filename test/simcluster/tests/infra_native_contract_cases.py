"""Native instrument contracts; hermetic controls never certify a deployment."""
import copy
import importlib.util
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest import mock

LIB = Path(__file__).resolve().parents[1] / 'drills/lib'
sys.path.insert(0, str(LIB))
import infra_owner_plan as plans
import infra_owner_ports as ports


def implementation():
    spec = importlib.util.spec_from_file_location('infra_native', LIB / 'infra_native.py')
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


class NativePortTests(unittest.TestCase):
    # origin: hot-update S9 plan §9.1 / §9.4; S9 INSTRUMENTS-3 per-port contracts
    def test_native_ports_are_implemented_and_refuse_missing_deployment_by_name(self):
        native = implementation()
        expected = {p for p in plans.PORTS if p.startswith(('readers.', 'tls.', 'faults.', 'exercises.', 'configs.'))}
        expected -= {'readers.effects'}
        expected |= {'owners.prepare', 'owners.ready'}
        self.assertEqual(set(native.PORTS), expected)
        for port in sorted(expected):
            with self.subTest(port=port), self.assertRaisesRegex(ValueError, '^NOT_RUN ' + port):
                native.execute({}, {}, port, [], 10)

    # origin: hot-update S9 plan §2.3 / §9.1; S9 INSTRUMENTS-3 direct native dispatch
    def test_native_owner_uses_the_builtin_producer_without_an_external_collector(self):
        with mock.patch('infra_native.execute', side_effect=ValueError('NOT_RUN owners.ready: deployment_inputs_required')) as call:
            with self.assertRaisesRegex(ValueError, 'deployment_inputs_required'):
                ports.native({'deployment': {}, 'supervisor': plans.contract.pin(os.environ['SIM_UPGRADE_SUPERVISOR'])}, {}, 'owners.ready', [])
            self.assertEqual(call.call_args.args[2:], ('owners.ready', [], 10))

    # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-3 config source custody
    def test_config_port_checks_source_and_requires_the_p26_hook_before_effect(self):
        native = implementation()
        with tempfile.TemporaryDirectory(prefix='infra-native-config-') as temporary:
            root = Path(temporary)
            source = root / 'config.json'; source.write_text('{"apps":{}}')
            plan = dict(configs={'valid': plans.contract.pin(source)})
            with self.assertRaisesRegex(ValueError, 'p26_config_stage_hook_required'):
                native.stage_config(plan, {}, {}, 'valid', [], 10)
            source.write_text('changed')
            with self.assertRaisesRegex(ValueError, 'missing_or_changed_pin'):
                native.stage_config(plan, {}, {}, 'valid', [], 10)

    # origin: hot-update S9 plan §9.5 / L22; S9 INSTRUMENTS-3 real bounded child statuses
    def test_native_command_retains_nonzero_and_timeout_receipts(self):
        native = implementation()
        with tempfile.TemporaryDirectory(prefix='infra-native-command-') as temporary:
            root = Path(temporary)
            script = root / 'operation.py'; script.write_text('raise SystemExit(23)\n')
            python = str(Path('/usr/bin/python3').resolve())
            spec = dict(argv=[python, '-I', '-B', str(script)], sha256=plans.contract.sha(python),
                        inputs=[plans.contract.pin(script)])
            plan = dict(supervisor=plans.contract.pin(os.environ['SIM_UPGRADE_SUPERVISOR']))
            rc, raw = native.run(plan, spec, [], 1)
            self.assertEqual((rc, raw), (23, b''))
            script.write_text('import time\ntime.sleep(10)\n'); spec['inputs'] = [plans.contract.pin(script)]
            rc, raw = native.run(plan, spec, [], .05)
            self.assertEqual((rc, raw), (124, b''))
            spec['inputs'][0]['sha256'] = '0' * 64
            with self.assertRaises(ValueError): native.run(plan, spec, [], 1)

    # origin: hot-update S9 plan §2.3 / §5.1; S9 INSTRUMENTS-3 held tuple join
    def test_tls_port_requires_exactly_one_kernel_accepted_socket(self):
        native = implementation()
        spec = importlib.util.spec_from_file_location('native_schema', Path(__file__).with_name('hot-update-infra-probe-test.py'))
        models = importlib.util.module_from_spec(spec); spec.loader.exec_module(models)
        with tempfile.TemporaryDirectory(prefix='infra-native-tls-') as temporary:
            handoff = Path(temporary) / 'handoff'; handoff.write_text('schema only')
            _, trace = models.complete_model(handoff, component='caddy')
            accepted = copy.deepcopy(trace['tls'][0]['accepted'])
            local, remote = accepted['Remote'], accepted['Local']
            census = dict(Sockets=[accepted])
            self.assertEqual(native.accepted_socket(census, local, remote, None), accepted)
            for value in (dict(Sockets=[]), dict(Sockets=[accepted, accepted])):
                with self.assertRaisesRegex(ValueError, 'accepted_socket_ambiguous'):
                    native.accepted_socket(value, local, remote, None)
            with self.assertRaisesRegex(ValueError, 'accepted_socket_ambiguous'):
                native.accepted_socket(census, remote, local, None)
            translation = dict(producer='kernel-conntrack-reader', generation=1, netns=accepted['NetNS'],
                original_local='192.0.2.1:10000', original_remote='192.0.2.2:443',
                translated_local=local, translated_remote=remote)
            self.assertEqual(native.accepted_socket(census, translation['original_local'],
                translation['original_remote'], translation), accepted)
            translation['netns'] += 1
            with self.assertRaises(ValueError):
                native.accepted_socket(census, translation['original_local'], translation['original_remote'], translation)

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-3 receipt privacy
    def test_native_runner_cannot_publish_an_unknown_or_private_receipt(self):
        native = implementation()
        for port in native.PORTS:
            with self.subTest(port=port), self.assertRaises((ValueError, KeyError, TypeError)):
                ports.public(port, {'private_key': 'never-public'})

    # origin: hot-update S9 plan §9.1 / §9.4; S9 INSTRUMENTS-3 per-port routing
    def test_every_native_port_preserves_its_handler_refusal(self):
        import infra_native
        import infra_native_actions
        import infra_native_targets
        config = {'schema': 'infra-native-deployment-v1'}
        ctx = {'mode': 'live'}
        for port in infra_native.PORTS:
            label = 'NOT_RUN ' + port + ': observed_missing_precondition'
            kwargs = {}
            if port.startswith('tls.'): target = 'infra_native.tls_reader'
            elif port == 'readers.node': target = 'infra_native.node_observation'
            elif port.startswith('configs.'): target = 'infra_native.stage_config'
            elif port.startswith(('faults.', 'exercises.')): target = 'infra_native_actions.action'
            else: target = 'infra_native_targets.observation'
            arguments = ['brk1', 'nats'] if port == 'readers.node' else []
            with self.subTest(port=port), mock.patch('infra_native.deployment', return_value=dict(config)), \
                 mock.patch('infra_native.inputs', return_value=[]), mock.patch(target, side_effect=ValueError(label)) as call:
                with self.assertRaisesRegex(ValueError, label):
                    infra_native.execute({}, ctx, port, arguments, 10)
                self.assertEqual(call.call_count, 1)

    # origin: hot-update S9 plan §2.2 / §9.1; S9 INSTRUMENTS-3 public receipts
    def test_native_receipts_never_persist_unvalidated_private_output(self):
        native = implementation()
        with tempfile.TemporaryDirectory(prefix='infra-native-receipt-') as temporary:
            root = Path(temporary)
            private = b'{"private_key":"must-never-be-persisted"}'
            native.record({'evidence': str(root)}, 'readers.sample', 23, private)
            files = list((root / 'native').iterdir())
            self.assertEqual(len(files), 1)
            row = plans.contract.read(files[0])
            self.assertEqual(row['native_rc'], 23)
            self.assertEqual(row['raw_sha256'], native.hashlib.sha256(private).hexdigest())
            self.assertNotIn(b'must-never-be-persisted', files[0].read_bytes())
            self.assertEqual(files[0].stat().st_mode & 0o777, 0o600)

    # origin: hot-update S9 plan §2.3 / §5.1; S9 INSTRUMENTS-3 conntrack join
    def test_conntrack_reader_rejects_absent_duplicate_and_wrong_original_tuples(self):
        from infra_native_targets import conntrack_translation
        raw = b'ipv4 2 tcp 6 431999 ESTABLISHED src=192.0.2.1 dst=192.0.2.2 sport=12345 dport=443 src=10.0.0.2 dst=10.0.0.1 sport=8443 dport=12345 [ASSURED] mark=0 use=1\n'
        actual = conntrack_translation(raw, '192.0.2.1:12345', '192.0.2.2:443', 17, 5)
        self.assertEqual(actual['translated_remote'], '10.0.0.2:8443')
        self.assertEqual(actual['translated_local'], '10.0.0.1:12345')
        self.assertEqual((actual['netns'], actual['generation']), (17, 5))
        for body, local, remote in ((b'', '192.0.2.1:12345', '192.0.2.2:443'),
            (raw + raw, '192.0.2.1:12345', '192.0.2.2:443'), (raw, '192.0.2.1:12346', '192.0.2.2:443')):
            with self.assertRaisesRegex(ValueError, 'conntrack_tuple_ambiguous'):
                conntrack_translation(body, local, remote, 17, 5)
        with self.assertRaisesRegex(ValueError, 'conntrack_namespace_required'):
            conntrack_translation(raw, '192.0.2.1:12345', '192.0.2.2:443', 0, 5)

    # origin: hot-update S9 plan §9.1 / §9.4; S9 INSTRUMENTS-3 native source pins
    def test_native_supply_pins_the_physical_and_transitive_execution_sources(self):
        native = implementation()
        with tempfile.TemporaryDirectory(prefix='infra-native-pins-') as temporary:
            source = Path(temporary) / 'deployment.json'
            executable = plans.contract.pin(Path('/usr/bin/python3').resolve())
            value = dict(schema='infra-native-deployment-v1', sim=executable, physical=executable)
            source.write_bytes(plans.contract.canonical(value))
            plan = dict(deployment=plans.contract.pin(source))
            rows = native.inputs(plan, 'readers.sample')
            paths = {row['path'] for row in rows}
            for name in ('infra_native.py', 'infra_flow_peer.py', 'fullstack_provision.py', 'flow_fixture_transport.sh'):
                self.assertIn(str(LIB / name), paths)
            self.assertIn(str(LIB.parents[3] / 'internal/infrahost/census_linux.go'), paths)
            source.write_text('{}')
            with self.assertRaisesRegex(ValueError, 'missing_or_changed_pin'):
                native.inputs(plan, 'readers.sample')

    # origin: hot-update S9 plan §9.1 / L22; S9 INSTRUMENTS-3 remote custody
    def test_remote_read_requires_registered_scope_and_remote_supervision(self):
        native = implementation()
        with self.assertRaisesRegex(ValueError, 'node_scope_required'):
            native.remote({}, {}, 'brk1', ['/usr/bin/true'])
        with self.assertRaisesRegex(ValueError, 'registered_remote_observer_required'):
            native.remote({}, {'nodes': {'brk1': {}}}, 'brk1', ['/usr/bin/true'])

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-3 existing fault primitives
    def test_network_fault_adapters_pin_the_real_drill_library(self):
        from infra_native_actions import network_spec, NETWORK
        sim = LIB.parents[1]
        plan = dict(flow=dict(here=str(sim), sim=plans.contract.pin(os.environ['SIM_UPGRADE_SUPERVISOR'])))
        for point in NETWORK:
            for operation in ('install', 'heal'):
                with self.subTest(point=point, operation=operation):
                    spec = network_spec(plan, point, operation, ['--nodes', 'brk1 brk2 brk3'])
                    plans.contract.spec(spec)
                    self.assertIn(point, spec['argv'])
                    self.assertIn(str(LIB / 'fault.sh'), {row['path'] for row in spec['inputs']})

    # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-3 catalog readback
    def test_config_staging_cannot_use_a_success_receipt_to_hide_wrong_catalog_bytes(self):
        native = implementation()
        with tempfile.TemporaryDirectory(prefix='infra-native-catalog-') as temporary:
            root = Path(temporary)
            source = root / 'config'; source.write_text('public input')
            plan = dict(configs={'valid': plans.contract.pin(source)})
            expected = dict(variant='valid', staged=True, sha256=plans.contract.sha(source))
            config = dict(config_stage={}, config_root=str(root))
            with mock.patch.object(native, 'invoke', return_value=(0, plans.contract.canonical(expected))), \
                 mock.patch.object(native, 'remote', return_value=b'0' * 64 + b' catalog'):
                with self.assertRaisesRegex(ValueError, 'product_catalog_readback_sha'):
                    native.stage_config(plan, config, {'nodes': ['brk1']}, 'valid', [], 10)
            with self.assertRaisesRegex(ValueError, 'stage_argument_binding'):
                native.stage_config(plan, config, {}, 'valid', ['--source', '/different'], 10)
            with self.assertRaisesRegex(ValueError, 'duplicate_stage_argument'):
                native.stage_config(plan, config, {}, 'valid', ['--variant', 'valid', '--variant', 'valid'], 10)

    # origin: hot-update S9 plan §9.1 / L22; S9 INSTRUMENTS-3 read-only owner sample
    def test_owner_sample_keeps_the_original_lease_and_does_not_drain_in_five_rounds(self):
        import time
        from flow_fixture_peer import start_supervised
        import infra_owner_lifecycle as lifecycle
        with tempfile.TemporaryDirectory(prefix='infra-native-sample-') as temporary:
            root = Path(temporary)
            script = root / 'lease.py'
            script.write_text("""import sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from infra_owner_lifecycle import Lease,signals
root=Path(sys.argv[2]); signals(); counter=[0]
def sample():
    counter[0]+=1
    return dict(counter=counter[0])
def drain(): (root/'drained').touch()
def close(): (root/'closed').touch()
Lease(root,'a'*64,1).serve(lambda:None,drain,close,lambda:None,sample)
""")
            before = len(os.listdir('/proc/self/fd'))
            for round in range(5):
                folder = root / str(round); folder.mkdir(mode=0o700)
                child = start_supervised([str(Path('/usr/bin/python3').resolve()), '-I', '-B', str(script), str(LIB), str(folder)])
                try:
                    deadline = time.monotonic() + 10
                    while not (folder / 'lease.json').exists():
                        self.assertIsNone(child.poll())
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(.01)
                    state = plans.contract.read(folder / 'lease.json')
                    with self.assertRaisesRegex(ValueError, 'stale_control'):
                        lifecycle.request(folder, 'b' * 64, 'sample')
                    self.assertEqual(lifecycle.request(folder, 'a' * 64, 'sample'), {'counter': 1})
                    self.assertEqual(lifecycle.request(folder, 'a' * 64, 'sample'), {'counter': 2})
                    self.assertEqual(lifecycle.birth(state['pid']), state['start_ticks'])
                    self.assertFalse((folder / 'drained').exists())
                    lifecycle.request(folder, 'a' * 64, 'stop')
                    self.assertEqual(child.wait(timeout=10), 0)
                    self.assertTrue((folder / 'drained').exists())
                    self.assertTrue((folder / 'closed').exists())
                    self.assertFalse(Path('/proc/' + str(state['pid'])).exists())
                finally:
                    if child.poll() is None:
                        child.terminate(); child.wait(timeout=10)
            self.assertEqual(len(os.listdir('/proc/self/fd')), before)

    # origin: hot-update S9 plan §10.2; S9 INSTRUMENTS-3 responsibility split
    def test_compatibility_module_split_retains_every_control_test(self):
        import ast
        from collections import Counter
        source = Path(__file__).with_name('compat_drill_cases.py')
        actual = Counter(node.name for node in ast.walk(ast.parse(source.read_text()))
            if isinstance(node, ast.FunctionDef) and node.name.startswith('test_'))
        expected = (
            'test_positive_reaches_expected_native_with_own_cli_statuses',
            'test_own_cli_status_is_load_bearing', 'test_every_required_check_is_load_bearing',
            'test_cleanup_that_leaves_owners_is_setup_red', 'test_execution_provenance_is_load_bearing',
            'test_frozen_template_binds_this_cell_and_verb', 'test_wrong_quadrant_executable_is_setup_red',
            'test_actual_parser_flags_are_required', 'test_parser_role_is_the_executed_role',
            'test_interrupting_scope_is_exact', 'test_late_create_requires_an_observed_live_fence',
            'test_full_profile_fixture_judge_and_custody_are_load_bearing', 'test_companion_command_is_required')
        self.assertEqual(actual, Counter(expected))

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-3 task projection contract only
    def test_task_sampler_keeps_observed_replacements_and_refuses_duplicate_tasks(self):
        import infra_owner_workloads as owners
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory(prefix='infra-native-task-shape-') as temporary:
            plan = dict(product=dict(effect_root=temporary, node='agt1', remote_python='/usr/bin/python3',
                binary=dict(path='/unexecuted/tether-candidate'), home=temporary, nats_url='nats://127.0.0.1:9'),
                exec=dict(remote_probe='/unexecuted/probe.py'))
            task = owners.Task(plan, {'nonce': 'ab' * 16}, Path(temporary), 'exec')
            task.job = SimpleNamespace(child=SimpleNamespace(target_pid=42), intact=lambda: None)
            row = dict(pid='observed-token', nid='agt1', argv=[task.remote_root])
            target = dict(alive=True, current=dict(pid=100, start_ticks=101), nonce='ab' * 16)
            with mock.patch.object(owners, 'run', return_value=plans.contract.canonical(dict(processes=[row]))), \
                 mock.patch.object(owners, 'remote', return_value=plans.contract.canonical(target)):
                first = task.snapshot()
                self.assertEqual((first['accepts'], first['redials'], first['ctl_pid']), (1, 0, 42))
                task.job.child.target_pid = 43
                self.assertEqual(task.snapshot()['redials'], 1)
            for tasks in ([], [row, row]):
                with mock.patch.object(owners, 'run', return_value=plans.contract.canonical(dict(processes=tasks))):
                    with self.assertRaisesRegex(ValueError, 'original_task_identity'):
                        task.snapshot()

    # origin: hot-update S9 plan §2.3 / §9.1; S9 INSTRUMENTS-3 sealed carrier boundary
    def test_flow_carrier_export_requires_the_existing_sealed_observer(self):
        from infra_flow_peer import implementation as peer_implementation
        from types import SimpleNamespace
        peer = peer_implementation()
        with tempfile.TemporaryDirectory(prefix='infra-native-carrier-') as temporary:
            owner = peer.Peer(Path(temporary), 'ab' * 16, 'client')
            owner.flows = {'tcp-0': SimpleNamespace(spec={'protocol': 'raw'})}
            try:
                with mock.patch.object(peer.observation, 'fetch', side_effect=ValueError('sealed_custody_missing')):
                    with self.assertRaisesRegex(ValueError, 'sealed_custody_missing'):
                        owner.command(dict(command='infra-bindings', target=dict(flows={'tcp-0': {'identities': []}})))
            finally:
                owner.server.close()

    # origin: hot-update S9 plan §2.3 / §9.1; S9 INSTRUMENTS-3 incomplete phase custody
    def test_sample_refuses_missing_physical_phase_export_before_any_operation(self):
        from infra_native_targets import observation
        ctx = dict(hops=[('nats', 'upgrade', 'brk1', 'ab' * 32)])
        with self.assertRaisesRegex(ValueError, 'physical_phase_export_required'):
            observation({}, {'repair_fence': {}}, ctx, 'readers.sample', ['hop-0', 'before'], 10)

    # origin: hot-update S9 plan lead LQ1; S9 INSTRUMENTS-3 before-effect platform fence
    def test_native_owner_refuses_unsupported_platform_before_reading_deployment(self):
        native = implementation()
        with mock.patch.object(native.platform, 'machine', return_value='aarch64'), \
             mock.patch.object(native, 'deployment') as called:
            with self.assertRaisesRegex(ValueError, '^NOT_RUN owners.prepare: unsupported$'):
                native.execute({}, {}, 'owners.prepare', [], 10)
            called.assert_not_called()


if __name__ == '__main__':
    unittest.main()
