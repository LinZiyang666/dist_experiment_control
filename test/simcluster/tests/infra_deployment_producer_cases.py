"""Adversarial producer contracts. Manufactured observations never certify a drill."""
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


def process(pid=41):
    return dict(BootID='boot', PID=pid, StartTicks=pid * 100, InvocationID='invocation',
        ExeDevice=1, ExeInode=pid, ExeSHA256='ab' * 32, Cgroup='/registered/unit')


def socket(owner):
    return dict(NetNS=1, Device=2, Inode=3, Cookie=4, Local='127.0.0.1:443',
                Remote='127.0.0.1:50000', Owner=owner)


class DeploymentProducerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='infra-deployment-producers-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.ctx = dict(evidence=str(self.root), nonce='ad' * 16, nodes=['brk1'],
                        hops=[['caddy', 'upgrade', 'brk1', '2.7.6']])
        (self.root / 'context.json').write_bytes(contract.canonical(self.ctx))
        self.old, self.main, self.warm = process(), process(42), process(43)
        self.census = dict(producer='kernel-reader', complete=True, generation=4,
            processes=[self.main], live_business=9, old_owner_pending=0)
        self.observed = dict(server=self.main, census=self.census, config='cd' * 32)
        self.batch = dict(context_sha256=contract.sha(self.root / 'context.json'), hop='hop-0',
            phase='retired', node='brk1', component='caddy', action='upgrade', tx='de' * 16,
            step=1, events=[dict(kind='started', at_ns=100, before=self.old, config='ac' * 32),
                dict(kind='temporary', at_ns=105, process=self.warm),
                dict(kind='main_ready', at_ns=110, socket=socket(self.main)),
                dict(kind='returned', at_ns=115), dict(kind='finished', at_ns=120, config='cd' * 32)])

    # origin: hot-update S9 plan §2.3 / §9.1; S9 INSTRUMENTS-5 physical phase
    def test_phase_preserves_original_times_and_independent_journal_revisions(self):
        phase = importlib.import_module('infra_deployment_phase')
        row = phase.project(self.ctx, 'hop-0', 'retired', self.batch, self.observed,
                            dict(tx='de' * 16, step=1, revision=8), dict(tx='de' * 16, step=1, revision=8),
                            [socket(self.main)], now_ns=130)
        self.assertEqual((row['started_ns'], row['finished_ns']), (100, 120))
        self.assertEqual(row['before'], self.old)
        self.assertEqual(row['temporary'], [self.warm])
        self.assertEqual(row['main_ready']['Owner'], self.main)
        self.assertEqual((row['root_revision'], row['transaction_revision']), (8, 8))

    # origin: hot-update S9 plan §2.3 / §9.1; S9 INSTRUMENTS-5 phase custody
    def test_phase_refuses_foreign_hops_status_only_times_and_shadow_ready(self):
        phase = importlib.import_module('infra_deployment_phase')
        for defect in ('context', 'hop', 'clock', 'duplicate-start', 'future', 'shadow', 'config', 'revision', 'before-alive'):
            batch, observed = copy.deepcopy(self.batch), copy.deepcopy(self.observed)
            root, tx = dict(tx='de' * 16, step=1, revision=8), dict(tx='de' * 16, step=1, revision=8)
            if defect == 'context': batch['context_sha256'] = '00' * 32
            if defect == 'hop': batch['hop'] = 'hop-1'
            if defect == 'clock': del batch['events'][0]['at_ns']
            if defect == 'duplicate-start': batch['events'].append(batch['events'][0])
            if defect == 'future': batch['events'][-1]['at_ns'] = 131
            if defect == 'shadow': batch['events'][2]['socket'] = socket(self.warm)
            if defect == 'config': observed['config'] = '00' * 32
            if defect == 'revision': root['revision'] = 7
            if defect == 'before-alive': observed['census']['processes'].append(self.old)
            with self.subTest(defect=defect), self.assertRaises((ValueError, KeyError)):
                phase.project(self.ctx, 'hop-0', 'retired', batch, observed, root, tx,
                              [socket(self.main)], now_ns=130)

    # origin: hot-update S9 plan §9.1 / L22; S9 INSTRUMENTS-5 source separation
    def test_phase_reads_three_actual_exports_and_fresh_physical_census(self):
        phase = importlib.import_module('infra_deployment_phase')
        import infra_native as native
        declarations = {key: dict(argv=['/unexecuted/' + key]) for key in ('events', 'root', 'transaction')}
        journal = dict(tx='de' * 16, step=1, revision=8)
        with mock.patch.object(phase, 'export', side_effect=[self.batch, journal, journal]) as read, \
             mock.patch.object(native, 'node_observation', return_value=self.observed) as observe, \
             mock.patch.object(native, 'physical', return_value=dict(Sockets=[socket(self.main)])) as physical, \
             mock.patch.object(phase.time, 'monotonic_ns', return_value=130):
            row = phase.read({}, dict(phase=declarations), self.ctx, 'hop-0', 'retired')
        self.assertEqual(read.call_count, 3)
        observe.assert_called_once()
        physical.assert_called_once()
        self.assertEqual(row['finished_ns'], 120)

    # origin: hot-update S9 plan §9.4 / P26; S9 INSTRUMENTS-5 immutable real inputs
    def test_staging_plan_is_private_immutable_and_contains_exact_actual_inputs(self):
        stage = importlib.import_module('infra_config_stage')
        config = self.staging_inputs()
        plan = self.root / 'stage-plan.json'
        stage.create_plan(config, plan)
        self.assertEqual(contract.read(plan), config)
        self.assertEqual(plan.stat().st_mode & 0o777, 0o400)
        ino = plan.stat().st_ino
        stage.create_plan(config, plan)
        self.assertEqual(plan.stat().st_ino, ino)
        changed = copy.deepcopy(config)
        changed['observers']['brk1']['root'] += '-other'
        with self.assertRaisesRegex(ValueError, 'immutable_plan_conflict'):
            stage.create_plan(changed, plan)
        self.assertEqual(contract.read(plan), config)

    def staging_inputs(self):
        sim = contract.pin(LIB.parents[1] / 'simcluster')
        public = self.root / 'public-config'; public.write_bytes(b'{"apps":{}}')
        return dict(schema='infra-config-stage-plan-v1',
            supervisor=contract.pin(os.environ['SIM_UPGRADE_SUPERVISOR']), sim=sim,
            flow=dict(here=str(LIB.parents[1]), sim=sim), nodes={'brk1': {}},
            observers={'brk1': dict(root='/tmp/registered-reader', cgroup='/registered/reader')},
            configs={key: contract.pin(public) for key in ('valid', 'invalid-config', 'invalid-cert')},
            config_root='/var/lib/tether-infra/configs')

    def producer_inputs(self):
        stage = self.staging_inputs()
        python = str(Path('/usr/bin/python3').resolve())
        # Unexecuted schema controls: these declarations are never a usable
        # live deployment or a successful injector/SS custody receipt.
        export = dict(argv=[python, '-I', '-B', str(LIB / 'infra_owner_ports.py')],
                      sha256=contract.sha(python), inputs=[contract.pin(LIB / 'infra_owner_ports.py')])
        action = dict(argv=[stage['sim']['path'], 'exec', 'brk1', '--', '/bin/false'],
                      sha256=stage['sim']['sha256'], inputs=[])
        isolated = self.root / 'home'; isolated.mkdir(exist_ok=True)
        from infra_deployment_faults import COMPOUND
        return dict(schema='infra-deployment-producers-v1', supervisor=stage['supervisor'],
            sim=stage['sim'], flow=stage['flow'], physical=contract.pin(os.environ['SIM_INFRA_TEST_BINARY']),
            product=dict(binary=contract.pin(os.environ['SIM_INFRA_TEST_BINARY']), home=str(isolated),
                nats_url='nats://127.0.0.1:9', node='agt1', read_path='/tmp/public-marker',
                effect_root='/tmp/public-effects', remote_python='/usr/bin/python3'),
            nodes={'brk1': {}}, observers=stage['observers'],
            phase={key: export for key in ('events', 'root', 'transaction')},
            nats_monitors={'brk1': 'http://127.0.0.1:8223'},
            fault_events={point: export for point in COMPOUND},
            fault_actions={point: dict(install=action, heal=action) for point in ('nats-n2', *COMPOUND)},
            final_sources=dict(business=export, controls=export, phase_lanes={name: export for name in
                ('writer_lost_ack', 'reader_partial_sync', 'commit_rename', 'terminal_courier')},
                flow=dict(custodian=export, evidence=str(self.root / 'flow'),
                          logs={name: export for name in ('broker', 'agent', 'client', 'target')})))

    # origin: hot-update S9 plan §9.4 / L22; S9 INSTRUMENTS-5 source graph
    def test_producer_plan_pins_its_complete_acyclic_graph_without_executing_it(self):
        deployment = importlib.import_module('infra_deployment_plan')
        stage = importlib.import_module('infra_config_stage')
        value = self.producer_inputs()
        source, staging = self.root / 'producers.json', self.root / 'staging.json'
        deployment.create_plan(value, source)
        stage.create_plan(self.staging_inputs(), staging)
        fields = deployment.configure(source, staging, dict(writer_lost_ack='small-push',
            reader_partial_sync='small-pull', commit_rename='large-push', terminal_courier='large-pull'),
            'hop-0', self.root / 'proof.json')
        self.assertEqual(set(fields['faults']), set(contract.verbs.ALL_FAULTS))
        self.assertEqual(set(fields['phase_lanes']), set(value['final_sources']['phase_lanes']))
        self.assertEqual(fields['config_root'], stage.CONFIG_ROOT)
        for spec in (fields['phase_reader'], fields['controls']['final'], fields['config_stage']):
            contract.spec(spec)
            pins = {pin['path'] for pin in spec['inputs']}
            self.assertNotIn(str(self.root / 'parent-owner.json'), pins)
            self.assertIn(str(LIB / 'infra_deployment_phase.py'), pins)
            self.assertIn(str(LIB / 'infra_deployment_final.py'), pins)
        value['phase'].pop('events')
        with self.assertRaisesRegex(ValueError, 'original_phase_and_journal_exports_required'):
            deployment.create_plan(value, self.root / 'missing-producers.json')
        self.assertFalse((self.root / 'missing-producers.json').exists())

    # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-5 all prerequisites
    def test_producer_plan_refuses_each_missing_dependency_before_publication(self):
        deployment = importlib.import_module('infra_deployment_plan')
        value = self.producer_inputs()
        paths = [('phase', key) for key in value['phase']]
        paths += [('fault_events', point) for point in value['fault_events']]
        paths += [('fault_actions', point, action) for point in value['fault_actions'] for action in ('install', 'heal')]
        paths += [('final_sources', key) for key in value['final_sources']]
        paths += [('nats_monitors', 'brk1')]
        for path in paths:
            broken = copy.deepcopy(value); parent = broken
            for key in path[:-1]: parent = parent[key]
            del parent[path[-1]]
            with self.subTest(path=path), self.assertRaises((ValueError, KeyError)):
                deployment.create_plan(broken, self.root / 'never-published.json')
            self.assertFalse((self.root / 'never-published.json').exists())

    # origin: hot-update S9 plan §9.4 / P26; S9 INSTRUMENTS-5 plan publication
    def test_staging_plan_refuses_changed_inputs_symlinks_and_hardlinks(self):
        stage = importlib.import_module('infra_config_stage')
        config = self.staging_inputs()
        config['configs']['valid']['sha256'] = '00' * 32
        with self.assertRaises(ValueError): stage.create_plan(config, self.root / 'absent.json')
        self.assertFalse((self.root / 'absent.json').exists())
        config = self.staging_inputs()
        plan = self.root / 'plan.json'; stage.create_plan(config, plan)
        link = self.root / 'link'; link.symlink_to(plan)
        with self.assertRaises((ValueError, OSError)): stage.create_plan(config, link)
        os.link(plan, self.root / 'alias')
        with self.assertRaises(ValueError): stage.create_plan(config, plan)

    # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-5 no-replace publication
    def test_plan_publication_never_replaces_a_competing_writer(self):
        stage = importlib.import_module('infra_config_stage')
        deployment = importlib.import_module('infra_deployment_plan')
        config = self.staging_inputs()
        source = self.root / 'competing.json'
        original_link = deployment.os.link
        def compete(*args, **kwargs):
            source.write_bytes(b'competing original bytes'); source.chmod(0o400)
            return original_link(*args, **kwargs)
        with mock.patch.object(deployment.os, 'link', side_effect=compete):
            with self.assertRaises(FileExistsError): stage.create_plan(config, source)
        self.assertEqual(source.read_bytes(), b'competing original bytes')
        self.assertFalse(any(path.name.startswith('.producer-plan-') for path in self.root.iterdir()))

    # origin: hot-update S9 plan §9.1 / P19; S9 INSTRUMENTS-5 four lane specs
    def test_all_four_lane_specs_bind_original_cohorts_and_writer_injector(self):
        supply = importlib.import_module('infra_deployment_plan')
        lanes = supply.lane_specs(dict(writer_lost_ack='small-push', reader_partial_sync='small-pull',
            commit_rename='large-push', terminal_courier='large-pull'), 'hop-0', self.root / 'lost-ack.json')
        self.assertEqual(set(lanes), {'writer_lost_ack', 'reader_partial_sync', 'commit_rename', 'terminal_courier'})
        for lane, spec in lanes.items():
            contract.spec(spec)
            self.assertIn(str(LIB / 'infra_s7_lanes.py'), spec['argv'])
            self.assertIn(str(LIB / 'infra_transfer_fixture.py'), {pin['path'] for pin in spec['inputs']})
            self.assertIn('--hop', spec['argv'])
            if lane == 'writer_lost_ack':
                self.assertEqual(spec['argv'][-2:], ['--lost-ack-proof', str(self.root / 'lost-ack.json')])
        with self.assertRaises(ValueError): supply.lane_specs({}, 'hop-0', self.root / 'proof')

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-5 no fabricated CLI success
    def test_producer_cli_names_absent_prerequisites_without_stdout(self):
        import infra_deployment_plan
        result = subprocess.run([os.environ['SIM_UPGRADE_SUPERVISOR'], '--', '/usr/bin/python3', '-I', '-B',
            str(LIB / 'infra_deployment_plan.py'), '--plan', str(self.root / 'missing'),
            '--plan-sha256', '00' * 32, '--context', str(self.root / 'context.json'), 'phase', 'hop-0', 'retired'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b'')
        self.assertIn(b'NOT_RUN deployment:', result.stderr)
        self.assertIn(b'missing_or_changed_pin', result.stderr)
        self.assertNotIn(b'Traceback', result.stderr)


    # origin: hot-update S9 plan §2.3; S9 INSTRUMENTS-5 original before census
    def test_before_phase_requires_the_original_incarnation_in_the_actual_census(self):
        import infra_deployment_phase as phase
        self.batch['phase'] = 'before'
        revision = dict(tx='de' * 16, step=1, revision=8)
        with self.assertRaisesRegex(ValueError, 'before_not_physical_original'):
            phase.project(self.ctx, 'hop-0', 'before', self.batch, self.observed,
                          revision, revision, [socket(self.main)], now_ns=130)

    # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-5 malformed CLI input
    def test_changed_plan_pin_is_named_not_run_without_a_traceback(self):
        source = self.root / 'changed-plan.json'; source.write_bytes(b'{}')
        result = subprocess.run([os.environ['SIM_UPGRADE_SUPERVISOR'], '--', '/usr/bin/python3', '-I', '-B',
            str(LIB / 'infra_deployment_plan.py'), '--plan', str(source), '--plan-sha256', '00' * 32,
            '--context', str(self.root / 'context.json'), 'phase', 'hop-0', 'before'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b'')
        self.assertIn(b'NOT_RUN deployment:', result.stderr)
        self.assertNotIn(b'Traceback', result.stderr)

    # origin: hot-update S9 plan §9.4 / L22; S9 INSTRUMENTS-5 failure boundary
    def test_producer_boundary_names_dependency_failures_without_exposing_their_body(self):
        import io
        from contextlib import redirect_stderr, redirect_stdout
        import infra_deployment_plan as deployment
        output, error = io.StringIO(), io.StringIO()
        argv = ['producer', '--plan', str(self.root / 'plan'), '--plan-sha256', '00' * 32,
                '--context', str(self.root / 'context.json'), 'phase', 'hop-0', 'before']
        with mock.patch.object(sys, 'argv', argv), redirect_stderr(error), redirect_stdout(output), \
             mock.patch.object(deployment, 'read_plan', side_effect=RuntimeError('dependency-body-must-stay-private')):
            self.assertEqual(deployment.main(), 2)
        self.assertEqual(output.getvalue(), '')
        self.assertIn('NOT_RUN deployment:', error.getvalue())
        self.assertNotIn('dependency-body-must-stay-private', error.getvalue())
        self.assertNotIn('Traceback', error.getvalue())


class FaultProducerTests(unittest.TestCase):
    def setUp(self):
        self.module = importlib.import_module('infra_deployment_faults')
        self.meta = dict(cluster_size=3, leader='n1', replicas=[dict(name='n2', current=True, lag=0),
                                                                   dict(name='n3', current=True, lag=0)])
        self.stream = dict(name='original', cluster=dict(leader='n1', replicas=[dict(name='n3', current=False, lag=9)]),
                           consumer_detail=[dict(name='reader', cluster=dict(leader='n1', replicas=[dict(name='n3', current=True, lag=0)]))])
        self.js = dict(server_id='server', meta_cluster=self.meta, streams=1, consumers=1, total=1,
                       account_details=[dict(id='$G', stream_detail=[self.stream])])

    # origin: hot-update S9 plan §3.2 / §9.1; S9 INSTRUMENTS-5 actual JS groups
    def test_fault_status_observes_meta_stream_and_consumer_groups_independently(self):
        status = self.module.network_status
        self.assertTrue(status('nats-stream-lag', [self.js])['active'])
        self.assertFalse(status('nats-consumer-lag', [self.js])['active'])
        self.stream['consumer_detail'][0]['cluster']['replicas'][0].update(current=False, lag=2)
        self.assertTrue(status('nats-consumer-lag', [self.js])['active'])
        self.assertFalse(status('nats-n2', [self.js])['active'])
        self.meta.update(cluster_size=2, replicas=self.meta['replicas'][:1])
        self.assertTrue(status('nats-n2', [self.js])['active'])
        self.meta.update(cluster_size=3, leader='', replicas=[dict(name='n2', current=False, offline=True),
                                                            dict(name='n3', current=False, offline=True)])
        self.assertTrue(status('nats-lost-quorum', [self.js])['active'])

    # origin: hot-update S9 plan §3.2; S9 INSTRUMENTS-5 unknown is never a hit
    def test_fault_status_refuses_unknown_or_truncated_asset_views(self):
        for defect in ('absent-meta', 'pagination', 'missing-consumer', 'unknown-current', 'no-observers', 'inconsistent-topology', 'meta-size'):
            js = copy.deepcopy(self.js); docs = [js]
            if defect == 'absent-meta': del js['meta_cluster']
            if defect == 'pagination': js['total'] = 2
            if defect == 'missing-consumer': del js['account_details'][0]['stream_detail'][0]['consumer_detail']
            if defect == 'unknown-current': del js['meta_cluster']['replicas'][0]['current']
            if defect == 'no-observers': docs = []
            if defect == 'meta-size': js['meta_cluster']['cluster_size'] = 4
            if defect == 'inconsistent-topology':
                other = copy.deepcopy(js); other['meta_cluster']['cluster_size'] = 2; docs.append(other)
            with self.subTest(defect=defect), self.assertRaises((ValueError, KeyError)):
                self.module.network_status('nats-stream-lag', docs)

    # origin: hot-update S9 plan §3.2; S9 INSTRUMENTS-5 stock JSz shape
    def test_monitor_total_counts_accounts_and_follower_meta_omission_is_unknown(self):
        self.js['streams'] = 2
        self.js['consumers'] = 2
        self.js['account_details'][0]['stream_detail'].append(copy.deepcopy(self.stream))
        self.js['account_details'][0]['stream_detail'][1]['name'] = 'other'
        del self.meta['replicas']  # Stock JSz omits this field on followers.
        self.assertTrue(self.module.network_status('nats-stream-lag', [self.js])['active'])
        self.assertFalse(self.module.network_status('nats-lost-quorum', [self.js])['active'])

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-5 actual monitor namespace
    def test_monitor_requires_local_endpoint_and_binds_actual_pid_and_server_id(self):
        import infra_native as native
        observed = dict(process=process())
        config = dict(nats_monitors={'brk1': 'http://127.0.0.1:8223'}, nodes={'brk1': dict(units={'nats': 'nats.service'})})
        body = dict(varz=dict(pid=41, server_id='server'), js=self.js)
        with mock.patch.object(native, 'physical', side_effect=[observed, process()]), \
             mock.patch.object(native, 'remote', return_value=contract.canonical(body)):
            self.assertEqual(self.module.monitor({}, config, {}, 'brk1'), self.js)
        body['varz']['pid'] = 99
        with mock.patch.object(native, 'physical', side_effect=[observed, process()]), \
             mock.patch.object(native, 'remote', return_value=contract.canonical(body)):
            with self.assertRaisesRegex(ValueError, 'monitor_not_executing_main'):
                self.module.monitor({}, config, {}, 'brk1')
        config['nats_monitors']['brk1'] = 'http://public.example:8223'
        with mock.patch.object(native, 'remote') as remote:
            with self.assertRaisesRegex(ValueError, 'local_public_monitor_required'):
                self.module.monitor({}, config, {}, 'brk1')
        remote.assert_not_called()

    def event(self, point):
        return dict(point=point, request_digest='ab' * 32, sent_ns=100, effect_ns=110, reply_cut_ns=120,
            facts=(dict(ldm_applied_ns=110) if point == 'after-ldm-reply-cut' else
                   dict(warm_copy_finished_ns=111, old_writer_exit_ns=113,
                        late_write_ns=112, copy_started_ns=113, copy_finished_ns=2_000_000_114,
                        written_sha256='cd' * 32, copied_sha256='cd' * 32) if point == 'acme-late-write-final-copy-over-2s' else
                   dict(main_failure_ns=110, controller_death_ns=111, shadow=process(43),
                        main_wait=dict(process=process(41), at_ns=110, wait_status=9, reaped=True),
                        controller_wait=dict(process=process(42), at_ns=111, wait_status=9, reaped=True),
                        samples=[dict(at_ns=112 + i * 1_000_000_000, process=process(43), serving=True) for i in range(122)])))

    # origin: hot-update S9 plan §4.2 / §5.3 / §5.4; S9 INSTRUMENTS-5 compound windows
    def test_compound_fault_hits_require_real_effect_cut_late_copy_and_shadow_duration(self):
        for point in self.module.COMPOUND:
            event = self.event(point)
            status = self.module.compound_status(point, [event], now_ns=200_000_000_000)
            self.assertTrue(status['active'])
            self.assertEqual(status['hits'], 1)
            self.assertTrue(all(status[key] for key in ('sent', 'effect', 'reply_cut')))
            self.assertIsNotNone(status['digest'])
            for field in ('sent_ns', 'effect_ns', 'reply_cut_ns', 'request_digest'):
                broken = copy.deepcopy(event); broken.pop(field)
                with self.subTest(point=point, field=field), self.assertRaises((ValueError, KeyError)):
                    self.module.compound_status(point, [broken], now_ns=200_000_000_000)
        point = 'acme-late-write-final-copy-over-2s'; event = self.event(point)
        event['facts']['copy_finished_ns'] = 2_000_000_113
        with self.assertRaises(ValueError): self.module.compound_status(point, [event], now_ns=200_000_000_000)
        event = self.event('main-failure-controller-death-shadow-over-120s')
        event['facts']['samples'][-1]['serving'] = False
        with self.assertRaises(ValueError): self.module.compound_status(event['point'], [event], now_ns=200_000_000_000)
        event = self.event('main-failure-controller-death-shadow-over-120s')
        for i, sample in enumerate(event['facts']['samples']):
            sample['at_ns'] = 112 + i * 120_000_000_000 // 121
        with self.assertRaises(ValueError): self.module.compound_status(event['point'], [event], now_ns=200_000_000_000)
        event = self.event('main-failure-controller-death-shadow-over-120s')
        event['facts']['controller_wait']['reaped'] = False
        with self.assertRaises(ValueError): self.module.compound_status(event['point'], [event], now_ns=200_000_000_000)
        event = self.event('acme-late-write-final-copy-over-2s')
        event['facts']['copied_sha256'] = 'ef' * 32
        with self.assertRaises(ValueError): self.module.compound_status(event['point'], [event], now_ns=200_000_000_000)
        event = self.event('after-ldm-reply-cut')
        event['facts']['ldm_applied_ns'] += 1
        with self.assertRaises(ValueError): self.module.compound_status(event['point'], [event], now_ns=200_000_000_000)
        event = self.event('after-ldm-reply-cut'); event['reply_cut_ns'] = 99
        with self.assertRaises(ValueError): self.module.compound_status(event['point'], [event], now_ns=200_000_000_000)
        event = self.event('after-ldm-reply-cut')
        with self.assertRaises(ValueError): self.module.compound_status(event['point'], [event, event], now_ns=200_000_000_000)

    # origin: hot-update S9 plan §9.1 / L22; S9 INSTRUMENTS-5 install custody
    def test_fault_install_needs_real_mechanism_and_independent_postcondition(self):
        import infra_native as native
        config = dict(sim=dict(path='/pinned/simcluster'), fault_actions={'nats-n2': {'install': dict(argv=['/pinned/simcluster', 'exec'])}})
        ctx = dict(nodes=['brk1', 'brk2', 'brk3'])
        with mock.patch.object(native, 'invoke', return_value=(0, b'')) as invoke, \
             mock.patch.object(self.module, 'status', return_value=dict(active=False)):
            with self.assertRaisesRegex(ValueError, 'postcondition'):
                self.module.mechanism({}, config, ctx, 'nats-n2', 'install', [], 10)
        invoke.assert_called_once()
        with self.assertRaisesRegex(ValueError, 'mechanism_required'):
            self.module.mechanism({}, {}, ctx, 'after-ldm-reply-cut', 'install', [], 10)


class FinalProducerTests(unittest.TestCase):
    def setUp(self):
        self.module = importlib.import_module('infra_deployment_final')

    # origin: hot-update S9 plan §9.1 / S5 §6.6; S9 INSTRUMENTS-5 original raw custody
    def test_final_refuses_missing_raw_logs_controls_and_descriptor_custody(self):
        for missing in ('business', 'controls', 'phase_lanes', 'flow'):
            value = {key: {} for key in ('business', 'controls', 'phase_lanes', 'flow')}
            del value[missing]
            with self.subTest(missing=missing), self.assertRaisesRegex(ValueError, '^NOT_RUN readers.final:'):
                self.module.admit(value)
        with self.assertRaisesRegex(ValueError, 'original_s5_custodian_required'):
            self.module.admit(dict(business={}, controls={}, phase_lanes={}, flow={}))

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-5 typed source containers
    def test_final_refuses_malformed_log_and_lane_sources_before_using_their_values(self):
        for name, reason in (('logs', 'complete_s5_log_sources_required'),
                             ('phase_lanes', 'complete_phase_lane_sources_required')):
            value = dict(business={}, controls={}, phase_lanes={},
                flow=dict(custodian={}, evidence='/tmp/unexecuted-flow',
                          logs={key: {} for key in ('broker', 'agent', 'client', 'target')}))
            if name == 'logs': value['flow']['logs'] = []
            else: value['phase_lanes'] = []
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, reason):
                self.module.admit(value)

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-5 controls are measured effects
    def test_control_projection_refuses_ack_only_or_scheduled_expected_effects(self):
        for operation in self.module.CONTROLS:
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                self.module.control(dict(op=operation, at_ns=100, effect=True, subject='pty'), 0, 200)

    def measured_controls(self):
        target = process(42)
        facts = {
            'resize': dict(seq=4, ack=4, cols=91, rows=37, observed_cols=91, observed_rows=37),
            'signal': dict(seq=5, ack=5, signal=10, observed_signal=10, target=target, observed_target=target),
            'keepalive_lost_ack': dict(request_digest='ab' * 32, sent_ns=90, effect_ns=95, reply_cut_ns=99),
            'input': dict(sent_sha256='cd' * 32, applied_sha256='cd' * 32, sent_bytes=120, applied_bytes=120),
            'output': dict(produced_sha256='de' * 32, received_sha256='de' * 32, produced_bytes=130, received_bytes=130),
            'ps_victim': dict(task_id='original-victim', observed_task_id='original-victim', target=process(44), observed_target=process(44)),
            'kill_victim': dict(target=process(44), waited_target=process(44), wait_status=9, exited=True),
            'revoke_alice': dict(revision=8, applied_revision=8, revoked=True, replayed_bytes=0)}
        return [dict(op=op, at_ns=100, facts=facts[op]) for op in self.module.CONTROLS]

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-5 eight measured effects
    def test_all_eight_controls_require_independent_target_effect_facts(self):
        for event in self.measured_controls():
            row = self.module.control(event, 80, 200)
            self.assertEqual(row['op'], event['op'])
            self.assertIs(row['effect'], True)
            self.assertEqual(row['producer'], 'control-target-reader')
            # Each observation channel has a distinct failure: ACK alone,
            # different OS child, byte mismatch, another victim, or replay.
            field, value = {
                'resize': ('observed_cols', 80), 'signal': ('observed_signal', 15),
                'keepalive_lost_ack': ('reply_cut_ns', 94), 'input': ('applied_bytes', 119),
                'output': ('received_sha256', 'ef' * 32), 'ps_victim': ('observed_task_id', 'other'),
                'kill_victim': ('exited', False), 'revoke_alice': ('replayed_bytes', 1)}[event['op']]
            broken = copy.deepcopy(event); broken['facts'][field] = value
            with self.subTest(op=event['op']), self.assertRaises(ValueError):
                self.module.control(broken, 80, 200)

    # origin: hot-update S9 plan §9.1 / S5 §6.6; S9 INSTRUMENTS-5 original oracle
    def test_local_flow_calls_original_s5_judge_and_refuses_remote_fd_numbers(self):
        import hot_update_flow_probe as probe
        with tempfile.TemporaryDirectory(prefix='infra-local-s5-') as temporary:
            root = Path(temporary)
            (root / 'evidence.json').write_bytes(contract.canonical(dict(nonce='ab' * 16,
                flows=[dict(protocol='ss', ss=dict(key_fd=987654321))])))
            with mock.patch.object(probe, 'judge', side_effect=probe.EvidenceError('private descriptor is not sealed')) as judge:
                with self.assertRaisesRegex(ValueError, 'original_s5_raw_or_descriptor_custody_required'):
                    self.module.local_flow(root, 'ab' * 16)
            judge.assert_called_once_with(root, 'ab' * 16, 'unchanged')
            with mock.patch.object(probe, 'judge', side_effect=probe.ContinuityError('duplex_bytes')):
                with self.assertRaisesRegex(ValueError, 's5_native_rc_1'):
                    self.module.local_flow(root, 'ab' * 16)

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-5 complete raw collection
    def test_final_composition_keeps_all_original_rows_lanes_and_raw_logs(self):
        self.compose_fixture()

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-5 final rejection
    def test_final_composition_refuses_changed_originals_context_lanes_or_control_targets(self):
        for defect in ('identity', 'bytes', 'control-context', 'signal-target', 'victim-target', 'alice', 'lanes', 'fin', 'native'):
            with self.subTest(defect=defect):
                self.compose_fixture(defect)

    def compose_fixture(self, defect=None):
        import infra_native as native
        with tempfile.TemporaryDirectory(prefix='infra-final-composition-') as temporary:
            root = Path(temporary); evidence = root / 'composer'; evidence.mkdir(mode=0o700)
            flow = evidence / 'flow'; flow.mkdir(mode=0o700)
            fixture = root / 'fixture'; fixture.mkdir()
            original = {name: dict(identity=(dict(application_nonce='ab' * 16, ctl=process(40),
                child=process(42 if name == 'pty' else 45), task_id='original-' + name) if name in ('pty', 'exec') else
                dict(application_nonce='ab' * 16, socket=socket(process(46)))), carrier='original-' + name)
                for name in contract.probe.ORIGINALS}
            manifest = dict(nonce='ab' * 16, originals=original, phase_lanes={key: key + '-original' for key in
                ('writer_lost_ack', 'reader_partial_sync', 'commit_rename', 'terminal_courier')})
            (evidence / 'context.json').write_bytes(contract.canonical(dict(nonce=manifest['nonce'])))
            (root / 'manifest.json').write_bytes(contract.canonical(manifest))
            (fixture / 'trace.json').write_bytes(contract.canonical(dict(nonce=manifest['nonce'],
                manifest_sha256=contract.probe.manifest_digest(manifest), t0_ns=80)))
            business = {name: dict(row, accepts=1, redials=0, tx=10, rx=10,
                tx_sha256='cd' * 32, rx_sha256='cd' * 32, fin=[True, True], native_rc=0) for name, row in original.items()}
            business['ss-alice']['revoked'] = True
            lanes = [dict(lane=lane, hop='hop-0', events=[], sent=False, effect=False, reply_cut=False)
                     for lane in manifest['phase_lanes']]
            sources = dict(business={'name': 'bytes'}, controls={'name': 'controls'},
                phase_lanes={row['lane']: dict(name=row['lane']) for row in lanes},
                flow=dict(custodian={'name': 'custodian'}, evidence=str(flow),
                          logs={name: dict(name=name) for name in ('broker', 'agent', 'client', 'target')}))
            events = self.measured_controls()
            context_sha256 = contract.sha(evidence / 'context.json')
            failures = dict(identity='final_original_or_bytes_changed', bytes='final_original_or_bytes_changed',
                **{'control-context': 'control_context_binding', 'signal-target': 'control_not_original_pty',
                   'victim-target': 'victim_not_separate_original', 'alice': 'final_alice_not_revoked',
                   'lanes': 'all_original_phase_lanes_required', 'fin': 'final_native_settlement_required',
                   'native': 'final_native_settlement_required'})
            if defect == 'identity': business['ssh']['identity'] = dict(business['ssh']['identity'], application_nonce='ef' * 16)
            if defect == 'bytes': business['ssh']['tx'] += 1
            if defect == 'control-context': context_sha256 = 'ef' * 32
            if defect == 'signal-target':
                events[1]['facts'].update(target=process(47), observed_target=process(47))
            if defect == 'victim-target':
                events[5]['facts'].update(target=process(42), observed_target=process(42))
                events[6]['facts'].update(target=process(42), waited_target=process(42))
            if defect == 'alice': business['ss-alice']['revoked'] = False
            if defect == 'lanes': lanes[1] = lanes[0]
            if defect == 'fin': business['exec']['fin'] = [True, False]
            if defect == 'native': business['exec']['native_rc'] = 1
            # Pure composition control: mocked inputs here are not a live
            # acceptance, a deployment injector or a substitute S5 oracle.
            with mock.patch.object(self.module, 'admit'), \
                 mock.patch.object(native, 'invoke', side_effect=[(0, b'{"rc":0}')] + [(0, b'original raw log\n')] * 4), \
                 mock.patch.object(self.module, 'source', side_effect=[business, dict(context_sha256=context_sha256, events=events), *lanes]), \
                 mock.patch.object(self.module, 'local_flow') as judge:
                if defect:
                    with self.assertRaisesRegex(ValueError, failures[defect]):
                        self.module.collect({}, dict(final_sources=sources),
                            dict(evidence=str(evidence), nonce=manifest['nonce']), [], 120)
                    judge.assert_not_called()
                    return
                result = self.module.collect({}, dict(final_sources=sources),
                    dict(evidence=str(evidence), nonce=manifest['nonce']), [], 120)
            self.assertEqual(result['business'], business)
            self.assertEqual(len(result['controls']), 8)
            self.assertEqual(result['phase_lanes'], lanes)
            judge.assert_called_once_with(str(flow), manifest['nonce'])
            for name in sources['flow']['logs']:
                self.assertEqual((flow / (name + '.raw.log')).read_bytes(), b'original raw log\n')
                self.assertEqual(contract.read(flow / (name + '-log-digest.json'))['length'], 17)

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-5 typed custody boundary
    def test_final_refuses_a_non_object_custodian_response_with_a_named_reason(self):
        import infra_native as native
        config = dict(final_sources=dict(flow=dict(custodian={})))
        with mock.patch.object(self.module, 'admit'), \
             mock.patch.object(native, 'invoke', return_value=(0, b'[]')):
            with self.assertRaisesRegex(ValueError, 'original_s5_custodian_reply_required'):
                self.module.collect({}, config, {}, [], 10)

    # origin: hot-update S9 plan §9.1 / S5 §6.6; S9 INSTRUMENTS-5 native rc
    def test_final_preserves_nonzero_custodian_status_and_does_not_publish_success(self):
        import infra_native as native
        config = dict(final_sources=dict(business={}, controls={}, phase_lanes={}, flow=dict(custodian={})))
        for rc, body_rc in ((1, 0), (0, 1)):
            with self.subTest(rc=rc, body_rc=body_rc), mock.patch.object(self.module, 'admit'), \
                 mock.patch.object(native, 'invoke', return_value=(rc, contract.canonical(dict(rc=body_rc)))) as invoke:
                with self.assertRaisesRegex(ValueError, 's5_native_rc_1'):
                    self.module.collect({}, config, dict(), [], 10)
            invoke.assert_called_once()


if __name__ == '__main__':
    unittest.main()
