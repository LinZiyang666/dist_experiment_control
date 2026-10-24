"""Hermetic supply and custody controls; no deployment instrument is executed."""
import copy
import importlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

LIB = Path(__file__).resolve().parents[1] / 'drills/lib'
sys.path.insert(0, str(LIB))
import infra_composer_contract as contract
from infra_deployment_producer_cases import DeploymentProducerTests, FaultProducerTests, FinalProducerTests


def completion_cases(base, owner_base):
    class CompletedSupplyTests(unittest.TestCase):
        setUp, port, write = base.setUp, base.port, base.write

        # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-4 assembly entry
        def test_assemble_command_publishes_every_live_profile_without_executing_candidate(self):
            from hot_update_infra_workload import Supervisor
            self.write()
            rc, raw = Supervisor(os.environ['SIM_UPGRADE_SUPERVISOR']).run([
                '/usr/bin/python3', '-I', '-B', str(self.supplier.__file__), 'assemble',
                '--deployment', str(self.deployment), '--output', str(self.output),
                '--vendor', str(self.vendor)], 30)
            self.assertEqual(rc, 0, raw)
            value = self.contract.load(self.output)
            self.assertEqual(value['mode'], 'live')
            self.assertEqual(set(value['profiles']), self.contract.profile_keys())
            self.assertEqual(len(value['profiles']), 18)
            self.assertTrue((self.vendor / 'infra-fixture.json').is_file())

        # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-4 concrete owner graph
        def test_native_assembly_pins_every_owner_source_and_refuses_a_missing_export(self):
            owner = owner_base('runTest'); owner.setUp()
            self.addCleanup(owner.doCleanups)
            declaration = NativeAdmissionTests('runTest'); declaration.setUp()
            self.addCleanup(declaration.doCleanups)
            config = copy.deepcopy(declaration.config)
            config.update(schema='infra-native-deployment-v1', sim=self.contract.pin(self.binary),
                physical=self.contract.pin(self.binary), nodes={}, observers={}, tls={})
            path = self.root / 'native.json'; path.write_bytes(self.contract.canonical(config))
            plan = copy.deepcopy(owner.value)
            plan.pop('native'); plan['deployment'] = self.contract.pin(path)
            source = self.root / 'owner.json'; source.write_bytes(self.contract.canonical(plan))
            self.write()
            value = self.supplier.stage(self.deployment, self.output, self.vendor, source)
            self.assertEqual(set(value['profiles']), self.contract.profile_keys())
            self.assertEqual(value['mode'], 'live')
            # These are deliberately unexecuted schema declarations, never an
            # acceptance receipt or a substitute for a live deployment export.
            for spec in self.contract.all_specs(value):
                self.contract.spec(spec)
            paths = {row['path'] for row in value['readers']['sample']['inputs']}
            self.assertIn(str(LIB / 'infra_native_admission.py'), paths)
            self.assertIn(str(LIB / 'infra_config_stage.py'), paths)
            del config['controls']['final']
            path.write_bytes(self.contract.canonical(config))
            plan['deployment'] = self.contract.pin(path)
            source.write_bytes(self.contract.canonical(plan))
            with self.assertRaisesRegex(ValueError, '^NOT_RUN owners.prepare: s5_raw_final'):
                self.supplier.stage(self.deployment, self.output, self.vendor, source)
            self.assertFalse(self.output.exists())
            self.assertFalse((self.vendor / 'infra-fixture.json').exists())

    return CompletedSupplyTests


class NativeAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='infra-native-admission-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        executable = str(Path('/usr/bin/python3').resolve())
        self.spec = dict(argv=[executable, '-I', '-B', str(LIB / 'infra_owner_ports.py')],
            sha256=contract.sha(executable), inputs=[contract.pin(LIB / 'infra_owner_ports.py')])
        self.config = dict(participants={'participant': dict(node='agt1', ingress_node='brk1', reader=self.spec)},
            repair_fence=self.spec, phase_reader=self.spec, controls={'final': self.spec},
            phase_lanes={name: self.spec for name in ('writer_lost_ack', 'reader_partial_sync',
                                                    'commit_rename', 'terminal_courier')},
            faults={name: {action: self.spec for action in ('install', 'status', 'heal')}
                    for name in contract.verbs.ALL_FAULTS},
            exercises={name: {label: dict(command=self.spec, cleanup=self.spec) for label in labels}
                       for name, labels in {'store-double-open': ('second-launch', 'legacy-owner'),
                           'exact-release-join': ('mismatched-release', 'exact-release'),
                           'caddy-responsibility-gates': importlib.import_module('hot_update_infra_scope').GATES}.items()},
            config_stage=self.spec, config_root='/var/lib/tether-infra/configs')

    # origin: hot-update S9 plan §9.1 / §9.4; S9 INSTRUMENTS-4 complete graph
    def test_native_admission_requires_each_material_producer_before_publication(self):
        admission = importlib.import_module('infra_native_admission')
        admission.validate(self.config, 'owners.prepare')
        paths = [('participants',), ('repair_fence',), ('phase_reader',), ('controls', 'final'),
                 ('config_stage',), ('config_root',)]
        paths += [('phase_lanes', name) for name in self.config['phase_lanes']]
        paths += [('faults', name, action) for name in self.config['faults'] for action in ('status',)]
        paths += [('faults', name, action) for name in self.config['faults']
                  if name not in admission.NETWORK for action in ('install', 'heal')]
        paths += [('exercises', name, label, 'cleanup') for name, recipes in self.config['exercises'].items()
                  for label in recipes]
        for path in paths:
            value = copy.deepcopy(self.config)
            parent = value
            for key in path[:-1]: parent = parent[key]
            del parent[path[-1]]
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, '^NOT_RUN owners.prepare:'):
                admission.validate(value, 'owners.prepare')

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-4 fail before remote effects
    def test_native_prepare_admits_all_producers_before_first_remote_read(self):
        import infra_native_targets as targets
        import infra_native
        ctx = dict(nodes=['brk1'], component='nats', profile='default-single', hops=[])
        with mock.patch.object(infra_native, 'node_observation') as observed:
            with self.assertRaisesRegex(ValueError, '^NOT_RUN owners.prepare:'):
                targets.observation({}, {'nodes': {'brk1': {}}}, ctx, 'owners.prepare', [], 10)
            observed.assert_not_called()

    # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-4 product catalog selection
    def test_native_admission_refuses_instrument_owned_config_catalog(self):
        admission = importlib.import_module('infra_native_admission')
        self.config['config_root'] = str(self.root)
        with self.assertRaisesRegex(ValueError, 'product_config_catalog_required'):
            admission.validate(self.config, 'configs.valid')

    # origin: hot-update S9 plan §9.4 / P26; S9 INSTRUMENTS-4 staging scope
    def test_config_hook_receives_the_current_context_and_refuses_a_foreign_one(self):
        import infra_native as native
        source = self.root / 'config'; source.write_bytes(b'public config')
        pin = contract.pin(source)
        plan = dict(configs={'valid': pin})
        ctx = dict(evidence=str(self.root), nodes=['brk1'])
        config = dict(config_stage=self.spec, config_root='/var/lib/tether-infra/configs')
        result = dict(variant='valid', staged=True, sha256=pin['sha256'])
        with mock.patch.object(native, 'invoke', return_value=(0, contract.canonical(result))) as invoke, \
             mock.patch.object(native, 'remote', return_value=(pin['sha256'] + ' catalog').encode()):
            native.stage_config(plan, config, ctx, 'valid', [], 10)
            arguments = invoke.call_args.args[4]
            self.assertEqual(arguments.count('--context'), 1)
            self.assertEqual(arguments[arguments.index('--context') + 1], str(self.root / 'context.json'))
            with self.assertRaisesRegex(ValueError, 'stage_argument_binding'):
                native.stage_config(plan, config, ctx, 'valid', ['--context', '/foreign/context.json'], 10)


class ConfigCatalogTests(unittest.TestCase):
    def setUp(self):
        self.module = importlib.import_module('infra_config_stage')
        self.temporary = tempfile.TemporaryDirectory(prefix='infra-config-catalog-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.catalog = self.root / 'catalog'
        self.catalog.mkdir(mode=0o700)
        self.source = self.root / 'public-config.json'
        self.source.write_bytes(b'{"apps":{}}\n')
        self.digest = contract.sha(self.source)

    # origin: hot-update S9 plan §9.4 / P26; S9 INSTRUMENTS-4 immutable staging
    def test_catalog_stage_is_content_addressed_private_and_idempotent(self):
        before = len(os.listdir('/proc/self/fd'))
        for round in range(5):
            self.module.install_at(self.catalog, self.source, self.digest, os.geteuid())
            saved = self.catalog / self.digest
            self.assertEqual(saved.read_bytes(), self.source.read_bytes())
            self.assertEqual(saved.stat().st_mode & 0o777, 0o400)
            identity = saved.stat().st_ino
            self.module.install_at(self.catalog, self.source, self.digest, os.geteuid())
            self.assertEqual(saved.stat().st_ino, identity)
        self.assertEqual(len(os.listdir('/proc/self/fd')), before)

    # origin: hot-update S9 plan §9.4 / P26; S9 INSTRUMENTS-4 no overwrite escape
    def test_catalog_stage_refuses_changed_source_existing_bytes_and_symlinks(self):
        for defect in ('source-sha', 'source-symlink', 'destination-bytes', 'destination-symlink', 'directory-mode'):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory(dir=self.root) as temporary:
                root = Path(temporary)
                catalog = root / 'catalog'; catalog.mkdir(mode=0o700)
                source = self.source
                if defect == 'source-sha':
                    source = root / 'changed'; source.write_bytes(b'changed')
                if defect == 'source-symlink':
                    source = root / 'link'; source.symlink_to(self.source)
                if defect == 'destination-bytes': (catalog / self.digest).write_bytes(b'wrong bytes')
                if defect == 'destination-bytes': (catalog / self.digest).chmod(0o400)
                if defect == 'destination-symlink': (catalog / self.digest).symlink_to(self.source)
                if defect == 'directory-mode': catalog.chmod(0o777)
                with self.assertRaises((ValueError, OSError)):
                    self.module.install_at(catalog, source, self.digest, os.geteuid())
                if defect == 'destination-bytes':
                    self.assertEqual((catalog / self.digest).read_bytes(), b'wrong bytes')
                self.assertFalse(any(p.name.startswith('.pending-') for p in catalog.iterdir()))

    # origin: hot-update S9 plan §9.4 / P26; S9 INSTRUMENTS-4 concurrent catalog writer
    def test_catalog_stage_never_overwrites_a_competing_publication(self):
        original_link = self.module.os.link
        def compete(*args, **kwargs):
            target = self.catalog / self.digest
            target.write_bytes(b'competing bytes'); target.chmod(0o400)
            return original_link(*args, **kwargs)
        with mock.patch.object(self.module.os, 'link', side_effect=compete):
            with self.assertRaises(FileExistsError):
                self.module.install_at(self.catalog, self.source, self.digest, os.geteuid())
        self.assertEqual((self.catalog / self.digest).read_bytes(), b'competing bytes')
        self.assertEqual([path.name for path in self.catalog.iterdir()], [self.digest])

    # origin: hot-update S9 plan §9.4 / P26; S9 INSTRUMENTS-4 complete path walk
    def test_catalog_stage_refuses_symlink_parents_and_hardlinked_entries(self):
        alias = self.root / 'alias'; alias.symlink_to(self.catalog, target_is_directory=True)
        with self.assertRaises(OSError):
            self.module.install_at(alias, self.source, self.digest, os.geteuid())
        parent = self.root / 'parent'; parent.mkdir(mode=0o700)
        nested = parent / 'nested'; nested.mkdir(mode=0o700)
        linked = self.root / 'linked'; linked.symlink_to(parent, target_is_directory=True)
        with self.assertRaises(OSError):
            self.module.install_at(linked / 'nested', self.source, self.digest, os.geteuid())
        self.module.install_at(self.catalog, self.source, self.digest, os.geteuid())
        os.link(self.catalog / self.digest, self.root / 'second-name')
        with self.assertRaisesRegex(ValueError, 'catalog_conflict'):
            self.module.install_at(self.catalog, self.source, self.digest, os.geteuid())

    # origin: hot-update S9 plan §9.4 / P26; S9 INSTRUMENTS-4 source closure
    def test_config_hook_pins_its_plan_source_and_real_transport_closure(self):
        source = self.root / 'plan.json'
        sim = contract.pin(LIB.parents[1] / 'simcluster')
        source.write_bytes(contract.canonical(dict(schema='infra-config-stage-plan-v1',
            supervisor=contract.pin(os.environ['SIM_UPGRADE_SUPERVISOR']), sim=sim,
            flow=dict(here=str(LIB.parents[1]), sim=sim), nodes={'brk1': {}},
            observers={'brk1': dict(root='/tmp/registered-s9-staging', cgroup='/s9/staging')},
            configs={name: contract.pin(self.source) for name in ('valid', 'invalid-config', 'invalid-cert')},
            config_root=self.module.CONFIG_ROOT)))
        spec = self.module.spec(source)
        contract.spec(spec)
        self.assertIn('--plan-sha256', spec['argv'])
        inputs = {row['path'] for row in spec['inputs']}
        for path in (source, self.source, LIB / 'infra_config_stage.py', LIB / 'fullstack_provision.py',
                     LIB / 'flow_fixture_transport.sh'):
            self.assertIn(str(path), inputs)

    # origin: hot-update S9 plan §9.4 / P26; S9 INSTRUMENTS-4 isolated executable
    def test_config_hook_runs_in_isolated_python_and_names_absent_inputs(self):
        result = subprocess.run([os.environ['SIM_UPGRADE_SUPERVISOR'], '--', '/usr/bin/python3', '-I', '-B',
            str(LIB / 'infra_config_stage.py'), '--plan', str(self.root / 'absent-plan'),
            '--plan-sha256', 'ab' * 32, '--context', str(self.root / 'absent-context'),
            '--variant', 'valid', '--source', str(self.source), '--source-sha256', self.digest],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b'')
        self.assertIn(b'NOT_RUN configs: product_catalog_staging_failed', result.stderr)
        self.assertNotIn(b'Traceback', result.stderr)


if __name__ == '__main__':
    unittest.main()
