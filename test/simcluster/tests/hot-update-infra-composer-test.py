#!/usr/bin/env -S python3 -I -B
"""Hermetic composer contract: actual supervised owners, no deployment commands.

The owner double supplies target receipts, not an oracle verdict. The real
composer and P32 verb engine must validate its missing/stale/early-exit cases.
origin: S9 internal review s9-composer F1; hot-update S9 plan §9.1 / P19/P26/P30.
"""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

SIM = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class ComposerTest(unittest.TestCase):
    # origin: S9 internal review s9-composer F1
    def test_composer_entry_is_supplied(self):
        self.assertTrue((SIM / 'lib/infra-composer.py').is_file(), 'release supplier has no composer')
        self.assertTrue((SIM / 'drills/lib/hot_update_infra_composer.py').is_file(), 'live requiredset composer absent')

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='infra-composer-controls-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.here = self.root / 'simcluster'
        shutil.copytree(SIM / 'drills/lib', self.here / 'drills/lib')
        (self.here / 'lib').mkdir()
        for name in ('infra-composer.py', 'stage-infra.sh'):
            shutil.copy2(SIM / 'lib' / name, self.here / 'lib' / name)
        self.vendor = self.here / 'vendor'
        (self.vendor / 'infra').mkdir(parents=True)
        for name in ('nats-server-prev', 'nats-server-next', 'caddy-prev', 'caddy-next'):
            (self.vendor / name).write_text(name)
        shutil.copyfile(self.vendor / 'nats-server-prev', self.vendor / 'infra/nats-server-2.14.5')
        self.owner = self.root / 'owner.py'
        shutil.copy2(SIM / 'tests/infra_composer_owner_double.py', self.owner)
        self.handoff = self.root / 'handoff'
        self.handoff.write_text('HERMETIC S7 boundary stand-in; never a live acceptance')
        self.python = str(Path('/usr/bin/python3').resolve())
        self.supervisor = os.environ['SIM_UPGRADE_SUPERVISOR']
        self.ev = self.root / 'evidence-with-a-long-name-that-exceeds-the-unix-socket-path-limit'
        self.ev.mkdir(mode=0o700)
        self.cohort = self.ev / 'cohort'
        self.env = dict(os.environ, INSTANCE='composer-test', SIM_INFRA_COMPOSER_RUNS=str(self.root / 'sessions'))
        self.source = self.root / 'supply.json'
        self.jobs = []
        self.addCleanup(self.stop_jobs)
        # The established independent P32 model supplies only hermetic target
        # facts; this test still runs the real supplier, composer and validator.
        module_spec = importlib.util.spec_from_file_location('composer_models', SIM / 'tests/hot-update-infra-probe-test.py')
        models = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(models)
        m, _ = models.complete_model(self.handoff)
        s7 = self.root / 's7'
        s7.mkdir()
        manifest = s7 / 'manifest.json'
        manifest.write_text('{}')
        flow = self.root / 'flow'
        flow.mkdir()
        model = dict(schema='infra-cohort-v1', phase='ready', producer='composer', seconds=1200,
            originals=m['originals'], attachments=m['attachments'], transfers=m['transfers'],
            s7=dict(manifest=str(manifest), evidence=str(s7)), flow=dict(evidence=str(flow), nonce='ab' * 16),
            s7_manifest_sha256=digest(manifest), s7_reader_digest='a' * 64, phase_lanes={})
        for t in model['transfers']:
            t['rate_limit'] = 1024
        self.model = self.root / 'target-model.json'
        self.model.write_text(json.dumps(model))
        python_sha = digest(self.python)
        owner_inputs = [dict(path=str(p), sha256=digest(p)) for p in (self.owner, self.model)]
        spec = lambda operation: dict(argv=[self.python, '-I', '-B', str(self.owner), operation,
                    '--model', str(self.model)], sha256=python_sha, inputs=owner_inputs)
        sys.path.insert(0, str(self.here / 'drills/lib'))
        contract_spec = importlib.util.spec_from_file_location('composer_contract_test', self.here / 'drills/lib/infra_composer_contract.py')
        self.contract = importlib.util.module_from_spec(contract_spec)
        contract_spec.loader.exec_module(self.contract)
        profiles = {key: dict(seconds=1200, rates={name: 1024 for name in self.contract.probe.COHORTS},
            prepare=spec('prepare'), ready=spec('ready'), workloads={name: dict(keeper=spec('keeper'), stop=spec('stop'))
                for name in ('flow', 'pty', 'exec', 'transfers')}) for key in self.contract.profile_keys()}
        client = dict(spec('client'), mode='cli', expected_output='ok {logical_id}')
        self.value = dict(schema='infra-composer-supply-v1', mode='hermetic',
            handoff=dict(schema='transfer-v1', accepted_by='lead', files={name: dict(path=str(self.handoff),
                sha256=digest(self.handoff)) for name in ('executable', 'source', 'schema', 'positive', 'negative')}),
            profiles=profiles, commands={name: dict(client, argv=client['argv'] + ['{logical_id}'])
                for name in ('node_ls', 'ps', 'read', 'write', 'short_exec')},
            readers={name: spec('reader') for name in ('sample', 'progress', 'effects', 'final', 'node')},
            tls={name: dict(address='127.0.0.1', server_name='localhost', ca_file=str(self.handoff), reader=spec('reader'))
                for name in self.contract.probe.TLS_LANES},
            faults={name: {action: spec('reader') for action in ('install', 'status', 'heal')}
                for name in self.contract.verbs.ALL_FAULTS},
            exercises={name: spec('reader') for name in self.contract.verbs.ALL_EXERCISES},
            configs={name: spec('reader') for name in self.contract.verbs.ALL_CONFIGS})
        self.write_supply()

    def write_supply(self):
        self.source.write_text(json.dumps(self.value))

    def execute(self, argv, **kwargs):
        return subprocess.run([self.supervisor, '--', *map(str, argv)], env=self.env,
                              capture_output=True, text=True, timeout=40, **kwargs)

    def supplier(self, command='stage', *, allow=True):
        argv = [self.python, '-I', '-B', self.here / 'lib/infra-composer.py', command, '--vendor', self.vendor]
        if command == 'stage':
            argv += ['--supply', self.source]
        if allow:
            argv += ['--allow-hermetic']
        return self.execute(argv)

    def staged(self):
        result = self.supplier()
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads((self.vendor / 'infra-fixture.json').read_text())

    def verbs(self, *args):
        return self.execute([self.python, '-I', '-B', self.here / 'drills/lib/hot_update_infra_verbs.py',
                            *args, '--evidence', self.ev])

    def launch_keeper(self, knobs=None):
        self.binding = self.staged()
        result = self.verbs('preflight', '--component', 'nats', '--profile', 'lifetime-store-lock')
        self.assertEqual(result.returncode, 0, result.stderr)
        result = self.verbs('prepare', '--component', 'nats', '--topology', 'single', '--profile', 'lifetime-store-lock',
            '--nodes', 'brk1', '--agents', 'agt1,agt2', '--ctl', 'ctl1', '--initial-version', '2.14.5',
            '--carrier-transition', 'unchanged')
        self.assertEqual(result.returncode, 0, result.stderr)
        output = (self.root / 'keeper.log').open('wb')
        job = subprocess.Popen([self.supervisor, '--', self.python, '-I', '-B',
            str(self.here / 'drills/lib/hot_update_infra_verbs.py'), 'cohort-exec', '--evidence', str(self.ev)],
            env=self.env, stdout=output, stderr=subprocess.STDOUT)
        output.close()
        self.jobs.append(job)
        result = self.verbs('prepared')
        self.assertEqual(result.returncode, 0, (result.stderr, (self.root / 'keeper.log').read_text()))
        if knobs:
            (self.cohort / 'knobs.json').write_text(json.dumps(knobs))
        return job

    def start(self):
        return self.verbs('start', '--carrier-transition', 'unchanged', '--probe-rate', '10', '--request-budget', '5',
                          '--flow-budget', '5', '--transfer-budget', '10')

    def control(self, verb, root=None):
        return self.execute(self.binding['cohort']['control']['argv'] + ['--evidence', str(root or self.cohort), verb])

    def stop_jobs(self):
        for job in self.jobs:
            if job.poll() is None and (self.cohort / 'composer.sock').exists():
                self.control('stop')
            if job.poll() is None:
                job.terminate()
            try:
                job.wait(timeout=15)
            except subprocess.TimeoutExpired:
                job.kill()
                job.wait(timeout=10)

    # origin: S9 internal review s9-composer F1 / binding schema and source custody
    def test_composer_stages_the_exact_p32_binding_and_rejects_stale_pins(self):
        binding = self.staged()
        self.assertEqual(set(binding), {'schema', 'mode', 'handoff', 'cohort', 'commands', 'readers', 'tls',
                                        'faults', 'exercises', 'configs'})
        self.assertEqual(binding['schema'], 'infra-binding-v1')
        self.assertEqual(binding['mode'], 'hermetic')
        self.assertEqual(self.supplier('verify').returncode, 0)
        self.owner.write_text(self.owner.read_text() + '\n# stale adapter\n')
        self.assertEqual(self.supplier('verify').returncode, 2)
        result = self.verbs('preflight', '--component', 'nats', '--profile', 'default-single')
        self.assertEqual(result.returncode, 2)
        self.assertIn('reader_source_bytes_changed', result.stderr)

    # origin: S9 internal review s9-composer F1 / no fabricated live acceptance
    def test_composer_refuses_missing_workloads_handoff_and_hermetic_promotion(self):
        result = self.supplier(allow=False)
        self.assertEqual(result.returncode, 2)
        self.assertFalse((self.vendor / 'infra-fixture.json').exists())
        for defect in ('workload', 'handoff', 'rate', 'profile', 'instrument'):
            with self.subTest(defect=defect):
                value = copy.deepcopy(self.value)
                if defect == 'workload':
                    next(iter(value['profiles'].values()))['workloads'].pop('pty')
                elif defect == 'handoff':
                    value['handoff']['files'].pop('negative')
                elif defect == 'rate':
                    next(iter(value['profiles'].values()))['rates']['small-push'] = 1000000
                elif defect == 'profile':
                    value['profiles'].pop(next(iter(value['profiles'])))
                else:
                    value['faults'].pop(next(iter(value['faults'])))
                self.source.write_text(json.dumps(value))
                self.assertEqual(self.supplier().returncode, 2)
                self.assertFalse((self.vendor / 'infra-fixture.json').exists())

    # origin: S9 internal review s9-composer F1 / original requiredset and supervised custody
    def test_composer_launches_original_owners_once_and_joins_them(self):
        job = self.launch_keeper()
        result = self.start()
        self.assertEqual(result.returncode, 0, (result.stderr, (self.root / 'keeper.log').read_text()))
        ready = json.loads((self.cohort / 'ready.json').read_text())
        self.assertEqual(set(ready['originals']), set(self.contract.probe.ORIGINALS))
        self.assertEqual({t['cohort'] for t in ready['transfers']}, set(self.contract.probe.COHORTS))
        pids = [int((self.cohort / name / 'pid').read_text()) for name in ('flow', 'pty', 'exec', 'transfers')]
        for pid in pids:
            os.kill(pid, 0)
        foreign = self.root / 'foreign'
        foreign.mkdir(mode=0o700)
        self.assertEqual(self.control('stop', foreign).returncode, 2)
        self.assertIsNone(job.poll())
        self.assertEqual(self.control('launch').returncode, 2)
        job.wait(timeout=15)
        # A duplicate launch fails the cohort rather than replacing originals.
        for pid in pids:
            self.assertFalse(Path('/proc', str(pid)).exists(), 'owner survived join')

    # origin: S9 internal review s9-composer F1 / required workload absent must be red
    def test_composer_never_publishes_ready_for_missing_or_changed_originals(self):
        # Each failed run owns a fresh isolated instance, never a replacement.
        for knob in ({'missing_original': 'pty'}, {'missing_transfer': True}, {'new_rate': True},
                     {'new_manifest': True}, {'redial': True}, {'exit_owner': 'exec'}):
            with self.subTest(knob=knob):
                self.launch_keeper(knob)
                result = self.start()
                self.assertEqual(result.returncode, 2, result.stdout)
                self.assertFalse((self.cohort / 'ready.json').exists(), 'fabricated READY after owner failure')
                self.jobs[-1].wait(timeout=15)
                self.assertEqual(self.jobs[-1].returncode, 2)
                shutil.rmtree(self.ev)
                self.ev.mkdir(mode=0o700)

    # origin: S9 internal review s9-composer F1 / attempt sink remains the product's descriptor
    def test_composer_exec_preserves_the_original_attempt_sink(self):
        job = self.launch_keeper()
        self.assertEqual(self.start().returncode, 0)
        spec = self.root / 'client.json'
        spec.write_text(json.dumps(self.binding['commands']['read']))
        receipt = self.root / 'attempts.jsonl'
        result = self.execute([self.python, '-I', '-B', self.here / 'drills/lib/hot_update_infra_workload.py',
            'exec-client', '--spec', spec, '--logical-id', 'stable-17', '--receipt', receipt])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), 'ok stable-17')
        rows = [json.loads(line) for line in receipt.read_text().splitlines()]
        self.assertEqual([r['logical_id'] for r in rows], ['stable-17', 'stable-17'])
        self.assertEqual(rows[-1]['kind'], 'observer_close')
        self.assertEqual(self.control('stop').returncode, 0)
        self.assertEqual(job.wait(timeout=15), 0)

    # origin: S9 internal review s9-composer F1 / a live label cannot replace S7 proof
    def test_composer_live_ready_requires_the_actual_s7_reader(self):
        self.value['mode'] = 'live'
        self.write_supply()
        job = self.launch_keeper()
        self.assertEqual(self.start().returncode, 2)
        self.assertFalse((self.cohort / 'ready.json').exists())
        self.assertEqual(job.wait(timeout=15), 2)

    # origin: S9 internal review s9-composer F1 / cleanup cannot hide a native owner failure
    def test_composer_keeps_owner_native_failure_red(self):
        job = self.launch_keeper({'stop_rc': 7})
        self.assertEqual(self.start().returncode, 0)
        self.assertEqual(self.control('stop').returncode, 2)
        self.assertEqual(job.wait(timeout=15), 2)
        for name in ('flow', 'pty', 'exec', 'transfers'):
            self.assertEqual((self.cohort / name / 'owner.rc').read_text().strip(), '7')
        self.assertEqual(json.loads((self.cohort / 'state.json').read_text())['phase'], 'failed')

    # origin: S9 internal review s9-composer F1 / shared pidfd/subreaper owner-death custody
    def test_composer_parent_death_reaps_every_original_owner(self):
        for round in range(5):
            with self.subTest(round=round):
                self.env['INSTANCE'] = 'death-round-' + str(round)
                job = self.launch_keeper()
                self.assertEqual(self.start().returncode, 0)
                state = json.loads((self.cohort / 'state.json').read_text())
                pids = [int((self.cohort / name / 'pid').read_text()) for name in ('flow', 'pty', 'exec', 'transfers')]
                os.kill(state['pid'], 9)
                self.assertNotEqual(job.wait(timeout=15), 0)
                for pid in pids:
                    self.assertFalse(Path('/proc', str(pid)).exists(), 'owner outlived its composer')
                result = self.execute(self.binding['readers']['node']['argv'] + ['brk1', 'nats'])
                self.assertEqual(result.returncode, 2, 'dead session admitted an instrument')
                shutil.rmtree(self.ev)
                self.ev.mkdir(mode=0o700)

    # origin: S9 internal review s9-composer F1 / cached hashes cannot bless rewritten sources
    def test_composer_input_pins_are_checked_before_any_launch(self):
        spec = self.value['readers']['node']
        self.contract.spec(spec)
        self.owner.write_text(self.owner.read_text() + '\n# changed owner bytes\n')
        with self.assertRaisesRegex(ValueError, 'composer_pin_bytes'):
            self.contract.spec(spec)

    # origin: S9 internal review s9-composer F1 / post-READY owner loss is sticky
    def test_composer_detects_an_owner_exit_after_ready(self):
        job = self.launch_keeper()
        self.assertEqual(self.start().returncode, 0)
        pid = int((self.cohort / 'pty/pid').read_text())
        os.kill(pid, 9)
        deadline = time.monotonic() + 8
        while job.poll() is None and time.monotonic() < deadline:
            time.sleep(.02)
        self.assertIsNotNone(job.poll(), 'composer ignored the loss of an original owner')
        self.assertEqual(job.returncode, 2)
        self.assertTrue((self.cohort / 'failure.json').is_file())
        result = self.execute(self.binding['readers']['progress']['argv'])
        self.assertEqual(result.returncode, 2)

    # origin: S9 internal review s9-composer F1 / P26 release supplier must stage the binding
    def test_composer_release_supplier_stages_and_verifies_the_binding(self):
        # Only the artifact build/download boundary is a double. The shell
        # supplier and composer validation/staging are the production files.
        marker = self.root / 'artifact-stage-called'
        artifact = self.here / 'lib/infra-artifact.py'
        artifact.write_text("from pathlib import Path\nPath(" + repr(str(marker)) + ").touch()\n")
        repo = self.root / 'repo'
        (repo / 'test').mkdir(parents=True)
        (repo / 'test/simcluster').symlink_to(self.here, target_is_directory=True)
        script = ('. "$1"; sim_stage_log() { :; }; SIM_REPO=$2; SIM_VENDOR=$3; '
                  'SIM_INFRA_PREVIOUS_REF=fixture; SIM_INFRA_COMPOSER_SUPPLY=$4; sim_stage_infra')
        args = ['/bin/sh', '-c', script, 'stage-test', self.here / 'lib/stage-infra.sh', repo, self.vendor]
        refused = self.execute(args + [''])
        self.assertEqual(refused.returncode, 2)
        self.assertFalse(marker.exists(), 'artifact effects preceded accepted owner supply')
        self.value['mode'] = 'live'
        self.write_supply()
        staged = self.execute(args + [self.source])
        self.assertEqual(staged.returncode, 0, staged.stderr)
        self.assertTrue(marker.exists())
        self.assertEqual(self.supplier('verify', allow=False).returncode, 0)
        binding = json.loads((self.vendor / 'infra-fixture.json').read_text())
        self.assertEqual(binding['schema'], 'infra-binding-v1')
        self.assertEqual(binding['mode'], 'live')
        # This is a supplier protocol test. The earlier live-ready control
        # independently proves that these hermetic facts cannot pass S7 LIVE.


if __name__ == '__main__':
    unittest.main()
