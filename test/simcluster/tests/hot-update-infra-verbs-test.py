#!/usr/bin/env -S python3 -I -B
"""Hermetic contract controls for the S9 fixture verb engine and scoped oracle.

origin: S9 FIX-INFRAVERBS (r9d finding 5); hot-update S9 plan §9.1 / §9.3 / §12 P32.

The arm controls (tests/hot-update-infra-arm-test.py) drive every verb through
real drills. These controls pin what no drill text exercises: the closed
argument grammar, evidence custody, verb order, the pinned-budget and binding
freeze, the NOT_RUN owner named while the live composer is absent, and the
scoped log oracle's append/panic rules. The library is copied into a private
tree so a staged vendor/ in the checkout can never satisfy or mask a check.
No deployment, drill, docker, network or installed tether is involved.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

TESTS = Path(__file__).resolve().parent
LIB = TESTS.parent / 'drills' / 'lib'
FILES = ('hot_update_infra.sh', 'hot_update_infra_verbs.py', 'hot_update_infra_scope.py', 'hot_update_infra_workload.py',
         'hot_update_infra_probe.py', 'transfer_observation.py', 'hot_update_budget.sh')
_spec = importlib.util.spec_from_file_location('infra_probe_test', TESTS / 'hot-update-infra-probe-test.py')
models = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(models)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class InfraVerbsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='tether-infra-verbs-')
        self.addCleanup(self.directory.cleanup)
        self.here = Path(self.directory.name) / 'simcluster'
        (self.here / 'drills/lib').mkdir(parents=True)
        for name in FILES:
            shutil.copy2(LIB / name, self.here / 'drills/lib' / name)
        self.ev = Path(self.directory.name) / 'evidence'
        self.ev.mkdir(mode=0o700)

    def verbs(self, *args):
        result = subprocess.run([os.environ['SIM_UPGRADE_SUPERVISOR'], '--', sys.executable, '-I', '-B',
                                 str(self.here / 'drills/lib/hot_update_infra_verbs.py')] + [str(a) for a in args],
                                capture_output=True, text=True, timeout=60)
        return result.returncode, result.stdout, result.stderr

    def refused(self, code, *args):
        rc, _, error = self.verbs(*args)
        self.assertEqual(rc, 2, (args, error))
        self.assertIn(code, error, args)

    def supply(self, binding_mode='live'):
        """A pinned stand-in composer: every instrument is /bin/true, nothing runs."""
        vendor = self.here / 'vendor'
        (vendor / 'infra').mkdir(parents=True)
        for alias, version in (('prev', '2.14.5'), ('next', '2.14.6')):
            for path in (vendor / ('nats-server-' + alias), vendor / 'infra' / ('nats-server-' + version)):
                path.write_text('hermetic nats ' + version + '\n')
        handoff = vendor / 'handoff.txt'
        handoff.write_text('hermetic handoff stand-in\n')
        spec = dict(argv=['/bin/true'], sha256=sha('/bin/true'))
        binding = dict(schema='infra-binding-v1', mode=binding_mode,
                       handoff=dict(accepted_by='lead', schema='transfer-v1', files={
                           k: dict(path=str(handoff), sha256=sha(handoff))
                           for k in ('executable', 'source', 'schema', 'positive', 'negative')}),
                       cohort=dict(keeper=spec, control=spec), tls={},
                       commands={k: dict(spec, mode='cli') for k in ('node_ls', 'ps', 'read', 'write', 'short_exec')},
                       readers=dict({k: spec for k in ('sample', 'progress', 'effects', 'final')},
                                    node=dict(argv=['/bin/echo', '{}'], sha256=sha('/bin/echo'))),
                       faults={}, exercises={}, configs={})
        (vendor / 'infra-fixture.json').write_text(json.dumps(binding))
        return vendor / 'infra-fixture.json'

    def prepared(self):
        self.supply()
        self.assertEqual(self.verbs('preflight', '--evidence', self.ev, '--component', 'nats', '--profile',
                                    'default-single')[0], 0)
        rc, _, error = self.verbs('prepare', '--evidence', self.ev, '--component', 'nats', '--topology', 'single',
                                  '--profile', 'default-single', '--nodes', 'brk1', '--agents', 'agt1,agt2', '--ctl',
                                  'ctl1', '--carrier-transition', 'unchanged', '--initial-version', '2.14.5')
        self.assertEqual(rc, 0, error)
        # Stand in for the composer keeper's own prepared receipt.
        state = json.loads((self.ev / 'fixture/state.json').read_text())
        (self.ev / 'cohort').mkdir(mode=0o700)
        (self.ev / 'cohort/prepared.json').write_text(json.dumps(dict(
            schema='infra-cohort-v1', phase='prepared', producer='composer', component='nats', topology='single',
            profile='default-single', initial_sha256=state['initial_sha256'])))
        self.assertEqual(self.verbs('prepared', '--evidence', self.ev)[0], 0)

    # origin: S9 FIX-INFRAVERBS / r9d finding 5
    def test_infra_verbs_refuse_undeclared_grammar(self):
        self.refused('unknown_component_profile', 'preflight', '--evidence', self.ev, '--component', 'nats',
                     '--profile', 'bogus')
        self.refused('verb_arguments', 'preflight', '--evidence', self.ev, '--component', 'nats', '--profile',
                     'default-single', '--nodes', 'brk1')
        self.refused('verb_arguments', 'check', '--evidence', self.ev)
        self.refused('verb_arguments', 'check', 'baseline', '--evidence', self.ev, '--component', 'nats')
        self.refused('unknown_direction', 'observe', '--evidence', self.ev, '--direction', 'sideways', '--nodes', 'brk1')
        self.refused('verb_arguments', 'heal', '--evidence', self.ev, '--point', 'nats-n2', '--scenario', 'x')
        self.refused('verb_arguments', 'finish', '--evidence', self.ev, '--point', 'nats-n2')
        rc, _, _ = self.verbs('checkpoint', '--evidence', self.ev)
        self.assertEqual(rc, 2)

    # origin: S9 FIX-INFRAVERBS / r9d finding 5
    def test_infra_verbs_hold_evidence_custody_and_order(self):
        self.ev.chmod(0o755)
        self.refused('evidence_custody', 'preflight', '--evidence', self.ev, '--component', 'nats', '--profile',
                     'default-single')
        self.ev.chmod(0o700)
        self.refused('evidence_custody', 'preflight', '--evidence', 'relative/evidence', '--component', 'nats',
                     '--profile', 'default-single')
        self.refused('fixture_not_preflighted', 'observe', '--evidence', self.ev, '--direction', 'upgrade', '--nodes', 'brk1')
        # The live path today: no composer supply, the pending owner is named.
        self.refused('NOT_RUN pending S9 P19/P30 live requiredset composer', 'preflight', '--evidence', self.ev,
                     '--component', 'nats', '--profile', 'default-single')
        row = json.loads((self.ev / 'fixture/verbs.jsonl').read_text().splitlines()[-1])
        self.assertEqual((row['verb'], row['rc']), ('preflight', 2))
        self.refused('verb_out_of_order', 'start', '--evidence', self.ev, '--carrier-transition', 'unchanged',
                     '--probe-rate', 10, '--request-budget', 5, '--flow-budget', 5, '--transfer-budget', 10)
        self.refused('fresh_evidence_required', 'preflight', '--evidence', self.ev, '--component', 'nats', '--profile',
                     'default-single')

    # origin: S9 FIX-INFRAVERBS / r9d finding 5
    def test_infra_prepare_freezes_topology_version_and_binding(self):
        self.supply()
        self.assertEqual(self.verbs('preflight', '--evidence', self.ev, '--component', 'nats', '--profile',
                                    'default-single')[0], 0)
        base = ['prepare', '--evidence', self.ev, '--component', 'nats', '--profile', 'default-single', '--agents',
                'agt1,agt2', '--ctl', 'ctl1', '--carrier-transition', 'unchanged']
        self.refused('prepare_topology', *base, '--topology', 'quorum', '--nodes', 'brk1 brk2 brk3', '--initial-version', '2.14.5')
        self.refused('prepare_topology', *base, '--topology', 'single', '--nodes', 'brk2', '--initial-version', '2.14.5')
        self.refused('initial_version_not_previous_adjacent', *base, '--topology', 'single', '--nodes', 'brk1',
                     '--initial-version', '2.14.6')
        binding = self.here / 'vendor/infra-fixture.json'
        original = binding.read_bytes()
        binding.write_bytes(original.replace(b'"live"', b'"hermetic"'))
        self.refused('binding_changed', *base, '--topology', 'single', '--nodes', 'brk1', '--initial-version', '2.14.5')
        binding.write_bytes(original)

    # origin: S9 FIX-INFRAVERBS / r9d finding 5
    def test_infra_start_never_widens_a_pinned_budget(self):
        self.prepared()
        pinned = ('--probe-rate', 10, '--request-budget', 5, '--flow-budget', 5, '--transfer-budget', 10)
        for name, value in (('--probe-rate', 9), ('--request-budget', 6), ('--flow-budget', 50), ('--transfer-budget', 11)):
            args = list(pinned)
            args[args.index(name) + 1] = value
            self.refused('start_budget_not_pinned', 'start', '--evidence', self.ev, '--carrier-transition', 'unchanged', *args)
        self.refused('start_budget_not_pinned', 'start', '--evidence', self.ev, '--carrier-transition', 'replaced', *pinned)
        # No stage advanced and the composer was never asked to launch.
        self.assertEqual(json.loads((self.ev / 'fixture/state.json').read_text())['stage'], 'prepared')

    # origin: S9 FIX-INFRAVERBS / r9d finding 5
    def test_infra_shell_adapter_binds_the_drill_evidence(self):
        foreign = Path(self.directory.name) / 'foreign'
        foreign.mkdir(mode=0o700)
        script = ('. "$HERE/drills/lib/hot_update_infra.sh"; INFRA_EV=$1; infra_fixture "$2" --evidence "$3" '
                  '--component nats --profile default-single')
        for verb, evidence, code in (('preflight', foreign, 2), ('preflight', self.ev, 2), ('teleport', self.ev, 2)):
            result = subprocess.run(['/bin/sh', '-c', script, 'adapter', str(self.ev), verb, str(evidence)],
                                    env=dict(os.environ, HERE=str(self.here)), capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, code, result.stderr)
        # A verb aimed at another root never starts a child there; the own root
        # reached the engine (NOT_RUN without composer supply), the bogus verb did not.
        self.assertFalse((foreign / 'fixture').exists())
        rows = [json.loads(line) for line in (self.ev / 'fixture/verbs.jsonl').read_text().splitlines()]
        self.assertEqual([(r['verb'], r['rc']) for r in rows], [('preflight', 2)])

    # origin: S9 FIX-INFRAVERBS / r9d finding 5
    def test_infra_manifest_schedule_shape_matches_the_arm(self):
        handoff = Path(self.directory.name) / 'handoff'
        handoff.write_bytes(b'hermetic handoff\n')
        manifest, _ = models.complete_model(handoff)
        for profile, ok in (('cycle', False), ('readiness-refusal', False), ('lifetime-store-lock', True),
                            ('legacy-capability', True)):
            empty = dict(manifest, hops=[], profile=profile, phase_lanes={})
            if ok:
                models.probe.validate_manifest(empty)
            else:
                with self.assertRaisesRegex(models.probe.EvidenceError, 'requiredset_hops'):
                    models.probe.validate_manifest(empty)
        # A no-roll schedule still cannot declare transfer phase lanes it can never cross.
        with self.assertRaisesRegex(models.probe.EvidenceError, 'transfer_phase_schedule'):
            models.probe.validate_manifest(dict(manifest, hops=[], profile='lifetime-store-lock'))
        # A healthy Caddy pair may complete a cycle (86.H); a NATS pair must defer (85.D).
        pair = [h for h in manifest['hops'] if h['host'] != 'brk3']
        models.probe.validate_manifest(dict(manifest, hops=[dict(h, component='caddy') for h in pair],
                                            tls_lanes=list(models.probe.TLS_LANES)))
        with self.assertRaisesRegex(models.probe.EvidenceError, 'full_adjacent_cycle'):
            models.probe.validate_manifest(dict(manifest, hops=pair))

    # origin: S9 FIX-INFRAVERBS / r9d finding 5. Every instrument and scope is
    # closed per arm profile; nothing outside it reaches a composer or oracle.
    def test_infra_started_verbs_refuse_undeclared_instruments(self):
        self.supply()
        self.assertEqual(self.verbs('preflight', '--evidence', self.ev, '--component', 'nats', '--profile',
                                    'default-single')[0], 0)
        path = self.ev / 'fixture/state.json'
        state = json.loads(path.read_text())
        path.write_text(json.dumps(dict(state, stage='started', topology='single', nodes=['brk1'], cursor=0)))
        handoff = Path(self.directory.name) / 'handoff'
        handoff.write_bytes(b'hermetic handoff\n')
        (self.ev / 'manifest.json').write_text(json.dumps(models.complete_model(handoff)[0]))
        log = self.ev / 'step-1.log'
        log.write_text('completed\n')
        cases = (('scope_arguments', 'check', 'round', '--evidence', self.ev),
                 ('scope_arguments', 'check', 'round', '--direction', 'upgrade', '--nodes', 'brk1', '--evidence', self.ev),
                 ('undeclared_scope', 'check', 'store-lock', '--evidence', self.ev),
                 ('undeclared_scope', 'check', 'refusal', '--point', 'nats-n2', '--command-rc', '75', '--command-log', log,
                  '--evidence', self.ev),
                 ('verb_out_of_order', 'check', 'logs', '--evidence', self.ev),
                 ('command_receipt', 'check', 'bootstrap', '--node', 'brk1', '--command-rc', '0', '--command-log',
                  '/etc/hostname', '--evidence', self.ev),
                 ('undeclared_fault', 'fault', '--evidence', self.ev, '--point', 'nats-n2'),
                 ('undeclared_exercise', 'exercise', '--evidence', self.ev, '--scenario', 'store-double-open'),
                 ('undeclared_config_variant', 'stage-config', '--evidence', self.ev, '--variant', 'valid'),
                 ('no_staged_config', 'config-digest', '--evidence', self.ev),
                 ('transaction_without_halt', 'transaction', '--sim', '/nonexistent/simcluster', '--evidence', self.ev),
                 ('heal_without_fault', 'heal', '--evidence', self.ev, '--point', 'nats-n2'),
                 ('observe_topology', 'observe', '--evidence', self.ev, '--direction', 'upgrade', '--nodes', 'brk2'),
                 # The next declared hop is an upgrade; a rollback is no declared refusal window.
                 ('undeclared_observation', 'observe', '--evidence', self.ev, '--direction', 'rollback', '--nodes', 'brk1'))
        for code, *args in cases:
            self.refused(code, *args)

    def scope(self, bundle):
        path = self.ev / 'bundle.json'
        path.write_text(json.dumps(bundle))
        result = subprocess.run([os.environ['SIM_UPGRADE_SUPERVISOR'], '--', sys.executable, '-I', '-B',
                                 str(self.here / 'drills/lib/hot_update_infra_scope.py'), 'judge', '--bundle', str(path)],
                                capture_output=True, text=True, timeout=120)
        return result.returncode, result.stdout

    # origin: S9 FIX-INFRAVERBS / r9d finding 5. Round checks see each block;
    # only the cycle scope sees the whole schedule return to the pinned version.
    def test_infra_cycle_scope_counts_hops_and_returns_to_pin(self):
        handoff = Path(self.directory.name) / 'handoff'
        handoff.write_bytes(b'hermetic handoff\n')
        manifest, trace = models.complete_model(handoff)
        pin = LIB / 'hot_update_infra_probe.py'  # complete_model's upgrade target image
        (self.here / 'vendor/infra').mkdir(parents=True)
        for path in (self.here / 'vendor/nats-server-next', self.here / 'vendor/infra/nats-server-2.14.6'):
            shutil.copy2(pin, path)
        (self.ev / 'manifest.json').write_text(json.dumps(manifest))
        (self.ev / 'trace.json').write_text(json.dumps(trace))
        ids = [h['id'] for h in manifest['hops']]
        nodes = ['brk1', 'brk2', 'brk3']

        def judge(expected=9, stale=None, cursor=9):
            observed = {}
            for index, node in enumerate(nodes):
                server = models.process(700 + index, image=sha(LIB / 'hot_update_infra.sh') if node == stale else sha(pin))
                row = dict(producer='kernel-reader', node=node, component='nats', at_ns=1, server=server,
                           invocation=dict(user='root', restart='on-failure', managed=True), config=None, serving=True,
                           temporary=[], broker=models.process(800 + index), infra_capable=True,
                           census=dict(producer='kernel-reader', complete=True, generation=1, processes=[server],
                                       live_business=1, old_owner_pending=0))
                (self.ev / (node + '.json')).write_text(json.dumps(row))
                observed[node] = str(self.ev / (node + '.json'))
            blocks = [dict(direction=d, nodes=nodes, hops=ids[i * 3:i * 3 + 3], window={})
                      for i, d in enumerate(('upgrade', 'rollback', 'reupgrade'))]
            return self.scope(dict(scope='cycle', arguments=dict(expected_hops=expected, final_version='2.14.6'),
                                   root=str(self.ev), manifest=str(self.ev / 'manifest.json'), trace=str(self.ev / 'trace.json'),
                                   state=dict(component='nats', nodes=nodes, cursor=cursor, blocks=blocks),
                                   snapshot=dict(watching=False, watch_failures=[]), observed=observed, fault_status={},
                                   here=str(self.here)))

        self.assertEqual(judge()[0], 0, judge()[1])
        for arguments, code in ((dict(expected=8), 'cycle_hop_count'), (dict(cursor=8), 'cycle_hop_count'),
                                (dict(stale='brk2'), 'not_returned_to_pin:brk2')):
            rc, output = judge(**arguments)
            self.assertEqual(rc, 1, (arguments, output))
            self.assertIn(code, output)

    # origin: S9 FIX-INFRAVERBS / r9d finding 5
    def test_infra_log_scope_requires_append_and_no_panic(self):
        handoff = Path(self.directory.name) / 'handoff'
        handoff.write_bytes(b'hermetic handoff\n')
        manifest, _ = models.complete_model(handoff)
        (self.ev / 'manifest.json').write_text(json.dumps(manifest))

        def judge(final_suffix, inode=7, remove=None):
            for phase, raw in (('baseline', b'log\n'), ('final', b'log\n' + final_suffix)):
                directory = self.ev / ('logs-' + phase)
                directory.mkdir(exist_ok=True)
                for node in ('brk1', 'agt1', 'agt2'):
                    for stream in (('agent_slog', 'agent_panic') if node.startswith('agt') else ('broker_slog', 'broker_panic')):
                        (directory / (node + '.' + stream + '.raw')).write_bytes(raw)
                        meta = dict(inode=inode if phase == 'final' else 7, length=len(raw), read_ok=True)
                        (directory / (node + '.' + stream + '.json')).write_text(json.dumps(meta))
            if remove:
                (self.ev / remove).unlink()
            bundle = self.ev / 'bundle.json'
            bundle.write_text(json.dumps(dict(scope='logs', arguments={}, root=str(self.ev), manifest=str(self.ev / 'manifest.json'),
                                              trace=str(self.ev / 'missing.json'), state=dict(log_topology='single', component='nats', nodes=['brk1']),
                                              snapshot=None, observed={}, fault_status={}, here=str(self.here))))
            result = subprocess.run([os.environ['SIM_UPGRADE_SUPERVISOR'], '--', sys.executable, '-I', '-B',
                                     str(self.here / 'drills/lib/hot_update_infra_scope.py'), 'judge', '--bundle', str(bundle)],
                                    capture_output=True, text=True, timeout=60)
            return result.returncode, result.stdout

        self.assertEqual(judge(b'more\n')[0], 0)
        rc, output = judge(b'panic: boom\ngoroutine 1 [running]:\n')
        self.assertEqual(rc, 1, output)
        self.assertIn('panic_in_window:', output)
        rc, output = judge(b'more\n', inode=8)
        self.assertEqual(rc, 1, output)
        self.assertIn('log_window_not_append:', output)
        self.assertEqual(judge(b'more\n', remove='logs-final/agt2.agent_panic.raw')[0], 2)


if __name__ == '__main__':
    unittest.main()
