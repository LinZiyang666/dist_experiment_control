#!/usr/bin/env python3
"""Two-sided arm controls for drills 85/86 and 39.NI/CI (r9d-nonvacuity D-11 sections).

origin: hot-update S9 plan §9.3 / §9.4 / P33 / T80; r9d D-11; S9 FIX-INFRAVERBS
(r9d finding 5: the controls used to replace the whole P32 seam because the
landed library lacked the verbs the drills call).

The REAL drill file runs for one arm: its case branch, every infra_drill_*
helper, drills/lib/cluster.sh grow fixtures, drills/lib/logs.sh
sim_log_snapshot, lib/assert.sh and drill_end, under the real FIX-ORPHAN
supervisor re-exec. The REAL fixture library runs every verb: the shell
adapter, the verb engine, the scoped oracle, the observer scheduler and the
FINAL probe (byte-identical copies, checked per run, so their own imports stay
inside the sandbox). Only the deployment boundary is doubled
(tests/hot_update_infra_arm_sandbox.py, tests/hot_update_infra_deployment_double.py):
$SIM, $DOCKER, the vendor verifiers, the release-source supplier, the live
requiredset composer of vendor/infra-fixture.json and the S7/S5 oracles behind
it. docker, ssh, sudo, systemctl, journalctl and tether are poisoned on PATH.
Arms with Caddy hops use a hermetic binding (loopback TLS lanes; the FINAL
result is labelled evidence_scope=hermetic); every other arm runs the library's
live branch, including the probe's own S7/S5 re-verification.

Each arm is driven (1) to its expected-verdicts.tsv verdict, with the merged
trace of library verbs, product/helper commands and composer instruments equal
to an independent per-arm obligation model and every verb's native status 0,
and (2) away from it by a fault aimed at one obligation, asserting the exact
native verdict and that the fault was reached. Nothing here is an infra cut,
roll, refusal or deployment receipt.

Usage: python3 -I -B tests/hot-update-infra-arm-test.py --arm <85.A|86.R|39.NI|...|all>
       [--results DIR] [--jobs N]
"""
import argparse
import functools
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

SIM = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location('hot_update_infra_arm_sandbox',
                                               Path(__file__).resolve().with_name('hot_update_infra_arm_sandbox.py'))
sandbox = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sandbox)
DRILLS = {'85': '85-hot-update-nats', '86': '86-hot-update-caddy', '39': '39-hot-update-legacy-n1'}
VERDICT = re.compile(r'^DRILL-VERDICT verdict=(GREEN|ASSERT-FAIL|SETUP-RED|PRODUCT-RED|INCOMPLETE) rc=([0-4]) '
                     r'assert_fail=(\d+) setup_red=(\d+) product_red=(\d+) not_covered=(\d+) nc_gap=(\d+) '
                     r'nc_guard=(\d+) pass=(\d+) -- (.*)$', re.M)
CODES = {'GREEN': 0, 'ASSERT-FAIL': 1, 'SETUP-RED': 2, 'PRODUCT-RED': 3, 'INCOMPLETE': 4}
TX32 = 'd' * 32
VERSIONS = sandbox.VERSIONS
NODES = {'single': ('brk1',), 'sole-voter': ('brk1',), 'pair': ('brk1', 'brk2'), 'quorum': ('brk1', 'brk2', 'brk3')}
HOPS = {'single': 3, 'sole-voter': 3, 'pair': 6, 'quorum': 9}
SEED = ('--account-seed', '/etc/tether/secrets/account.nk')
CLI_PREFIX = ['runuser', '-u', 'tether', '--', 'env', 'HOME=/var/lib/tether']
HERMETIC = {'86.R', '86.B', '86.I', '86.H', '86.F', '86.U'}  # arms with Caddy hops: loopback TLS lanes


def header_arms(name):
    values = re.findall(r'^# arms: (.*)$', (SIM / 'drills' / (name + '.sh')).read_text(), re.M)
    assert len(values) == 1, name + ': missing or duplicate # arms: header'
    return values[0].split()


def budget():
    values = dict(re.findall(r'^(HOT_UPDATE_[A-Z_]+)=([0-9]+)$', (SIM / 'drills/lib/hot_update_budget.sh').read_text(), re.M))
    return tuple(values[k] for k in ('HOT_UPDATE_PROBE_RATE', 'HOT_UPDATE_REQUEST_S', 'HOT_UPDATE_LIVE_FLOW_STALL_S',
                                     'HOT_UPDATE_TRANSFER_PROGRESS_S'))


def expected_verdicts():
    rows = {}
    for line in (SIM / 'expected-verdicts.tsv').read_text().splitlines():
        if line and not line.startswith('#'):
            rows[line.split('\t')[0]] = line.split('\t')[1]
    return rows


def unit_name(key):
    number, arm = key.split('.', 1)
    return DRILLS[number] + '.' + arm


def fx(verb, *args):
    """One library verb as its own ledger records it, in the drill's argument order."""
    if verb in ('check', 'transaction'):
        return ('fixture', verb) + args + ('--evidence', 'EV')
    return ('fixture', verb, '--evidence', 'EV') + args


class Model:
    """The arm's obligation sequence, written from the S9 drill and fixture contracts.

    Independent of the drill and library text: a dropped round, a wrong hop
    count, a CLI verb on the wrong binary, a missing composer instrument or a
    judge that no longer receives the command's real status changes the trace.
    """

    def __init__(self, component, topology, profile, grow=None, binary='/usr/local/bin/tether'):
        self.c, self.t, self.p, self.grow, self.bin = component, topology, profile, grow, binary
        self.nodes, self.log_nodes = NODES[topology], NODES[topology]
        self.version = VERSIONS[component][2]
        self.open = []

    def cli(self, *args, recover=None, status=None):
        single = self.t == 'single'
        if status is not None:
            return (('cli', 'brk1', '/usr/local/bin/tether', 'admin', 'infra', 'status', '--tx', status) if single else
                    ('cli', 'brk1', '/usr/local/bin/tether', 'cluster', 'upgrade', '--component', self.c, '--status',
                     status) + SEED)
        if recover is not None:
            if single:
                return ('cli', 'brk1', '/usr/local/bin/tether', 'admin', 'infra', 'recover', '--tx', recover)
            return ('cli', 'brk1', self.bin, 'cluster', 'upgrade', '--component', self.c, '--recover', recover) + SEED
        if single:
            return ('cli', 'brk1', self.bin, 'admin', 'infra', 'upgrade', '--component', self.c) + args
        return ('cli', 'brk1', self.bin, 'cluster', 'upgrade', '--component', self.c) + SEED + args

    def logs(self):
        return [('logs', n, s) for n in self.log_nodes + ('agt1', 'agt2')
                for s in (('agent_slog', 'agent_panic') if n.startswith('agt') else ('broker_slog', 'broker_panic'))]

    def prelude(self):
        events = [('artifact', 'verify', 'HERE/vendor', 'HERE/vendor/install.sh'),
                  ('image', 'check', 'HERE', 'HERE/vendor/infra-image-inputs.json'),
                  fx('preflight', '--component', self.c, '--profile', self.p), ('sim', 'nuke')]
        if self.grow is None:
            events.append(('sim', 'up', '--brokers', '1', '--agents', '2', '--ctl', '1'))
            events += [('sim', 'init', 'brk1')] if self.t == 'sole-voter' else []
        else:
            events += [('sim', 'up', '--brokers', str(len(self.nodes)), '--agents', '2', '--ctl', '1'), ('sim', 'init', 'brk1')]
            events += [('sim', 'grow', node) for node in self.nodes[1:]]
        prepare = ('--component', self.c, '--topology', self.t, '--profile', self.p, '--nodes', ' '.join(self.nodes),
                   '--agents', 'agt1,agt2', '--ctl', 'ctl1', '--carrier-transition', 'unchanged',
                   '--initial-version', VERSIONS[self.c][1])
        events += [fx('prepare', *prepare), ('supply', 'keeper', self.c, self.t, self.p, ' '.join(self.nodes)),
                   fx('prepared')] + self.logs()
        rate, request, flow, transfer = budget()
        events += [('supply', 'control', 'launch'),
                   fx('start', '--carrier-transition', 'unchanged', '--probe-rate', rate, '--request-budget', request,
                      '--flow-budget', flow, '--transfer-budget', transfer), fx('started'), fx('check', 'baseline')]
        if self.p != 'legacy-capability':
            for node in self.nodes:
                events += [('helper', node, 'bootstrap'),
                           fx('check', 'bootstrap', '--node', node, '--command-rc', '0', '--command-log', 'LOG')]
        return events

    def observe(self, direction):
        return fx('observe', '--direction', direction, '--nodes', ' '.join(self.nodes))

    def round(self, direction, rc='0'):
        args = ('--rollback',) if direction == 'rollback' else ('--to-version', self.version, '--expect-sha256', 'SHA')
        return [self.observe(direction), self.cli(*args),
                fx('check', 'round', '--direction', direction, '--nodes', ' '.join(self.nodes), '--command-rc', rc,
                   '--command-log', 'LOG')]

    def cycle(self):
        return self.round('upgrade') + self.round('rollback') + self.round('reupgrade') + [
            fx('check', 'cycle', '--expected-hops', str(HOPS[self.t]), '--final-version', self.version)]

    def finish(self):
        return [fx('check', 'retirement'), fx('finish')] + self.logs() + [fx('check', 'logs')]

    def close(self):
        return ([('supply', 'fault-heal', p, 'cleanup') for p in self.open] + [('supply', 'control', 'stop'),
                fx('stop'), fx('closed')])

    def fault(self, point):
        self.open.append(point)
        return [('supply', 'fault-install', point), fx('fault', '--point', point)]

    def refusal(self, point, rc, check_only=False):
        events = self.fault(point) + [self.cli('--to-version', self.version, '--expect-sha256', 'SHA'),
                                      ('supply', 'fault-status', point),
                                      fx('check', 'refusal', '--point', point, '--command-rc', rc, '--command-log', 'LOG'),
                                      ('supply', 'fault-heal', point)]
        self.open.remove(point)
        if point == 'nats-n2':
            events.append(fx('heal', '--point', point, '--recovery-topology', 'sole-voter'))
            self.t, self.nodes = 'sole-voter', ('brk1',)
        else:
            events.append(fx('heal', '--point', point))
        return events if check_only else events + self.round('upgrade')

    def halt(self, point, rc):
        return self.fault(point) + [self.observe('upgrade'), self.cli('--to-version', self.version, '--expect-sha256', 'SHA'),
                                    ('supply', 'fault-status', point),
                                    fx('check', 'halted', '--point', point, '--command-rc', rc, '--command-log', 'LOG'),
                                    self.cli(status='TX'), fx('transaction', '--sim', 'SIM'), self.cli(recover='TX'),
                                    fx('check', 'recovered', '--tx', 'TX', '--command-rc', '0', '--command-log', 'LOG')]

    def exercise(self, scenario, check):
        return [('supply', 'exercise', scenario), fx('exercise', '--scenario', scenario), fx('check', check)]

    def reload(self, variant, rc):
        events = [('supply', 'config', variant), fx('stage-config', '--variant', variant), fx('config-digest'),
                  self.observe('reload'), self.cli('--reload-config', '--config-sha256', 'CFG')]
        if variant == 'valid':
            return events + [fx('check', 'reload', '--config-sha256', 'CFG', '--command-rc', rc, '--command-log', 'LOG')]
        return events + [fx('check', 'invalid-config-cert', '--variant', variant, '--config-sha256', 'CFG',
                            '--command-rc', rc, '--command-log', 'LOG')]

    def support(self):
        events = []
        for action in ('update-support', 'rollback-support', 'update-support'):
            events += [self.observe(action), ('helper', 'brk1', action),
                       fx('check', 'support', '--action', action, '--command-rc', '0', '--command-log', 'LOG')]
        return events


def legacy_body(m):
    return [('artifacts', 'verify', 'v0.6.0', 'tether-prev', 'a3431a12be3cf947cc797c28fea93382fa68472d')] + m.prelude() + [
        fx('check', 'legacy-source'), m.cli('--to-version', m.version, '--expect-sha256', 'SHA'),
        fx('check', 'legacy-refusal', '--command-rc', '64', '--command-log', 'LOG')] + m.finish() + m.close()


def rule(tool, glob, rc, nth='-', output='-'):
    return ('rule', (tool, glob, str(rc), str(nth), output))


def knob(name, value=True):
    return ('knob', (name, value))


def cycled(m):
    return m.prelude() + m.cycle() + m.finish() + m.close()


REFUSED_ONCE = [rule('sim', '*--to-version*', 75, 1)]
HALTED_ONCE = [rule('sim', '*--to-version*', 74, 1)]
STUCK = 'host never leaves the old binary'
# Each arm: model, scenario faults for its positive, and negatives
# (description, faults, exact verdict, events that must stay absent).
ARMS = {
    '85.A': dict(model=lambda: Model('nats', 'single', 'default-single'), body=cycled, negatives=[
        ('rollback CLI rc 75 is not a completed roll', [rule('sim', '*--rollback*', 75)], 'ASSERT-FAIL'),
        ('old incarnation alive after the reupgrade hop', [knob('sample.old_alive', 'hop-2')], 'ASSERT-FAIL',
         [fx('check', 'cycle', '--expected-hops', '3', '--final-version', '2.14.6')]),
        ('management bootstrap leaves the stock unit unmanaged', [knob('node.unmanaged')], 'ASSERT-FAIL', [('fixture', 'observe')]),
        (STUCK, [knob('node.stuck', 'brk1')], 'ASSERT-FAIL', [('cli', 'brk1', '/usr/local/bin/tether', 'admin', 'infra',
                                                               'upgrade', '--component', 'nats', '--rollback')]),
        ('stock owner still in the kernel census at retirement', [knob('node.census_old', 'brk1')], 'ASSERT-FAIL',
         [('fixture', 'finish')])]),
    '85.A1': dict(model=lambda: Model('nats', 'sole-voter', 'standalone-js'), body=cycled, negatives=[
        ('sole-voter Raft init failure', [rule('sim', 'init brk1', 1)], 'SETUP-RED', [('fixture', 'prepare')]),
        ('cluster upgrade rc 75 on the reupgrade round', [rule('sim', '*cluster upgrade*--to-version*', 75, 2)], 'ASSERT-FAIL'),
        ('rollback hop never moves the attachment', [knob('sample.frozen_attachment', 'hop-1')], 'ASSERT-FAIL',
         [fx('observe', '--direction', 'reupgrade', '--nodes', 'brk1')])]),
    '85.B': dict(model=lambda: Model('nats', 'quorum', 'adjacent-cycle', grow='grow_to_3'), body=cycled, negatives=[
        ('declared grow fails before any business', [rule('sim', 'up --brokers 3*', 1)], 'SETUP-RED', [('fixture', 'prepare')]),
        ('rollback hop never moves the attachment', [knob('sample.frozen_attachment', 'hop-4')], 'ASSERT-FAIL',
         [fx('observe', '--direction', 'reupgrade', '--nodes', 'brk1 brk2 brk3')]),
        ('an original transfer settles before the first cut', [knob('s7.finish', 'large-pull')], 'INCOMPLETE')]),
    '85.D': dict(model=lambda: Model('nats', 'pair', 'readiness-refusal', grow='grow_to_2'), faults=REFUSED_ONCE,
                 body=lambda m: m.prelude() + m.refusal('nats-n2', '75') + m.finish() + m.close(), negatives=[
        ('N2 refusal restarted a server', [knob('node.restarted', 'brk2')], 'ASSERT-FAIL', [('fixture', 'heal')]),
        ('product shrink leaves a pair', [knob('heal.topology', 'pair')], 'SETUP-RED', [('fixture', 'observe')]),
        ('post-heal sole-voter roll rc 75', [rule('sim', '*cluster upgrade*--to-version*', 75, 2)], 'ASSERT-FAIL'),
        ('N2 join leader drift is not the declared pair', [rule('sim', 'status *', 0, '-', 'brk2')], 'SETUP-RED',
         [('fixture', 'prepare')])]),
    '85.Q': dict(model=lambda: Model('nats', 'quorum', 'readiness-refusal', grow='grow_to_3'), faults=REFUSED_ONCE,
                 body=lambda m: m.prelude() + m.refusal('nats-lost-quorum', '75') + m.finish() + m.close(), negatives=[
        ('lost-quorum fault cannot be installed', [knob('fault-install.fail', 'nats-lost-quorum')], 'SETUP-RED',
         [('fixture', 'check', 'refusal')]),
        ('lost-quorum instrument not active at the refusal', [knob('fault.inactive')], 'ASSERT-FAIL', [('fixture', 'heal')]),
        ('deferral output names no closed refusal code', [rule('sim', '*--to-version*', 75, 1, 'deferred')], 'ASSERT-FAIL',
         [('fixture', 'heal')]),
        ('unsafe cut accepted rc 0', [rule('sim', '*--to-version*', 0, 1)], 'ASSERT-FAIL', [('fixture', 'heal')])]),
    '85.L': dict(model=lambda: Model('nats', 'quorum', 'readiness-refusal', grow='grow_to_3'),
                 faults=[rule('sim', '*--to-version*', 75, 1), rule('sim', '*--to-version*', 75, 2)],
                 body=lambda m: m.prelude() + m.refusal('nats-stream-lag', '75', True) +
                 m.refusal('nats-consumer-lag', '75', True) + m.round('upgrade') + m.finish() + m.close(), negatives=[
        ('consumer-lag cut accepted rc 0', [rule('sim', '*--to-version*', 0, 2)], 'ASSERT-FAIL', [('fixture', 'observe')]),
        ('stream-lag heal fails', [knob('fault-heal.fail', 'nats-stream-lag')], 'SETUP-RED',
         [fx('fault', '--point', 'nats-consumer-lag')]),
        ('final roll rc 75 after both lag refusals', [rule('sim', '*--to-version*', 75, 3)], 'ASSERT-FAIL')]),
    '85.S': dict(model=lambda: Model('nats', 'single', 'lifetime-store-lock'),
                 body=lambda m: m.prelude() + m.exercise('store-double-open', 'store-lock') + m.finish() + m.close(), negatives=[
        ('second launch exercise cannot run', [knob('exercise.fail', 'store-double-open')], 'SETUP-RED', [('fixture', 'check', 'store-lock')]),
        ('second launch opened the store', [knob('exercise.outcome.second-launch', 'opened')], 'ASSERT-FAIL', [('fixture', 'finish')]),
        ('legacy owner guard never attempted', [knob('exercise.drop', 'legacy-owner')], 'INCOMPLETE', [('fixture', 'finish')])]),
    '85.J': dict(model=lambda: Model('nats', 'quorum', 'exact-release-join', grow='grow_to_3'),
                 body=lambda m: m.prelude() + m.exercise('exact-release-join', 'join') + m.finish() + m.close(), negatives=[
        ('join exercise cannot run', [knob('exercise.fail', 'exact-release-join')], 'SETUP-RED', [('fixture', 'check', 'join')]),
        ('mismatched release joined', [knob('exercise.outcome.mismatched-release', 'joined')], 'ASSERT-FAIL', [('fixture', 'finish')]),
        ('temporary broker never retired', [knob('exercise.unretired')], 'ASSERT-FAIL')]),
    '85.F': dict(model=lambda: Model('nats', 'quorum', 'post-ldm-recovery', grow='grow_to_3'), faults=HALTED_ONCE,
                 body=lambda m: m.prelude() + m.halt('after-ldm-reply-cut', '74') + m.finish() + m.close(), negatives=[
        ('halted output names no transaction', [rule('sim', '*--to-version*', 74, 1, 'infra_recovery_required')], 'ASSERT-FAIL',
         [('fixture', 'transaction')]),
        ('reply cut never hit the sent request', [knob('fault.not_hit')], 'ASSERT-FAIL', [('fixture', 'transaction')]),
        ('halt let a hop retire before recovery', [knob('sample.no_halt')], 'ASSERT-FAIL', [('fixture', 'transaction')]),
        ('product status receipt unavailable (P24 pending)', [rule('sim', '*--status *', 64, '-', 'unknown flag: --status')],
         'SETUP-RED', [('cli', 'brk1', '/usr/local/bin/tether', 'cluster', 'upgrade', '--component', 'nats', '--recover')]),
        ('product recover fails', [rule('sim', '*--recover*', 70)], 'ASSERT-FAIL', [('fixture', 'finish')])]),
    '86.R': dict(model=lambda: Model('caddy', 'single', 'config-reload'),
                 body=lambda m: m.prelude() + m.reload('valid', '0') + m.finish() + m.close(), negatives=[
        ('valid reload refused rc 64', [rule('sim', '*--reload-config*', 64)], 'ASSERT-FAIL'),
        ('reloaded generation is not the staged digest', [knob('config.sha', 'ab' * 32)], 'ASSERT-FAIL', [('fixture', 'finish')]),
        ('configuration input cannot be staged', [knob('config.fail', 'valid')], 'SETUP-RED', [('fixture', 'observe')]),
        ('reload replaced the Caddy process', [knob('node.restarted', 'brk1')], 'ASSERT-FAIL', [('fixture', 'finish')])]),
    '86.B': dict(model=lambda: Model('caddy', 'quorum', 'adjacent-cycle', grow='grow_to_3'), body=cycled, negatives=[
        ('declared grow fails', [rule('sim', 'up --brokers 3*', 1)], 'SETUP-RED', [('fixture', 'prepare')]),
        ('old Caddy alive after the first hop', [knob('sample.old_alive', 'hop-0')], 'ASSERT-FAIL'),
        ('upgrade round rc 75', [rule('sim', '*--to-version*', 75, 1)], 'ASSERT-FAIL')]),
    '86.I': dict(model=lambda: Model('caddy', 'single', 'stock-root-retrofit'),
                 body=lambda m: m.prelude() + m.fault('acme-late-write-final-copy-over-2s') + m.cycle() + m.finish() + m.close(),
                 negatives=[
        ('ACME late-write fault cannot be installed', [knob('fault-install.fail', 'acme-late-write-final-copy-over-2s')],
         'SETUP-RED', [('fixture', 'observe')]),
        ('retrofit bootstrap leaves the stock unit unmanaged', [knob('node.unmanaged')], 'ASSERT-FAIL', [('fixture', 'observe')]),
        ('first hop never moves the attachment', [knob('sample.frozen_attachment', 'hop-0')], 'ASSERT-FAIL'),
        ('retrofit bootstrap restarted the stock server', [knob('node.bootstrap_restart', 'brk1')], 'ASSERT-FAIL',
         [('fixture', 'observe')])]),
    '86.H': dict(model=lambda: Model('caddy', 'pair', 'healthy-n2', grow='grow_to_2'), body=cycled, negatives=[
        ('N2 leader drift is not the declared pair', [rule('sim', 'status *', 0, '-', 'brk2')], 'SETUP-RED', [('fixture', 'prepare')]),
        (STUCK, [knob('node.stuck', 'brk2')], 'ASSERT-FAIL', [fx('observe', '--direction', 'rollback', '--nodes', 'brk1 brk2')]),
        ('second-node bootstrap rc 1', [rule('sim', '*tether-infra-helper bootstrap', 1, 2)], 'ASSERT-FAIL', [('fixture', 'observe')])]),
    '86.C': dict(model=lambda: Model('caddy', 'single', 'invalid-config-cert'), faults=[rule('sim', '*--reload-config*', 64)],
                 body=lambda m: m.prelude() + m.reload('invalid-config', '64') + m.reload('invalid-cert', '64') + m.finish()
                 + m.close(), negatives=[
        ('invalid config accepted rc 0', [rule('sim', '*--reload-config*', 0, 1)], 'ASSERT-FAIL'),
        ('refused input still changed the generation', [knob('node.config_changed', 'brk1')], 'ASSERT-FAIL', [('fixture', 'finish')]),
        ('invalid certificate cannot be staged', [knob('config.fail', 'invalid-cert')], 'SETUP-RED')]),
    '86.K': dict(model=lambda: Model('caddy', 'single', 'responsibility-permission-gates'),
                 body=lambda m: m.prelude() + m.exercise('caddy-responsibility-gates', 'responsibility-gates') + m.finish()
                 + m.close(), negatives=[
        ('responsibility exercise cannot run', [knob('exercise.fail', 'caddy-responsibility-gates')], 'SETUP-RED',
         [('fixture', 'check', 'responsibility-gates')]),
        ('external storage gate never exercised', [knob('exercise.drop', 'external-storage')], 'INCOMPLETE', [('fixture', 'finish')]),
        ('live QUIC force-closed and signed', [knob('exercise.forced.live-quic')], 'ASSERT-FAIL')]),
    '86.F': dict(model=lambda: Model('caddy', 'single', 'shadow-recovery'), faults=HALTED_ONCE,
                 body=lambda m: m.prelude() + m.halt('main-failure-controller-death-shadow-over-120s', '74') + m.finish()
                 + m.close(), negatives=[
        ('controller death never hit', [knob('fault.not_hit')], 'ASSERT-FAIL', [('fixture', 'transaction')]),
        ('shadow stopped serving past the deadline', [knob('node.not_serving', 'brk1')], 'ASSERT-FAIL', [('fixture', 'transaction')]),
        ('product status says the halted transaction completed', [rule('sim', '* status --tx *', 0, '-', json.dumps(dict(
            Tx=[0xdd] * 16, Step=1, Phase=9, Revision=1, Code='', Detail='', Before={}, Current={}, Census={},
            Proof=[1] * 32), separators=(',', ':')))], 'SETUP-RED', [('cli', 'brk1', '/usr/local/bin/tether', 'admin', 'infra', 'recover')]),
        ('product recover fails', [rule('sim', '* recover *', 70)], 'ASSERT-FAIL', [('fixture', 'finish')])]),
    '86.U': dict(model=lambda: Model('caddy', 'single', 'support-two-generation'),
                 body=lambda m: m.prelude() + m.support() + m.cycle() + m.finish() + m.close(), negatives=[
        ('rollback-support helper rc 1', [rule('sim', '*tether-infra-helper rollback-support', 1)], 'ASSERT-FAIL',
         [fx('observe', '--direction', 'upgrade', '--nodes', 'brk1')]),
        ('rollback-support never moves the attachment', [knob('sample.frozen_attachment', 'hop-1')], 'ASSERT-FAIL'),
        (STUCK, [knob('node.stuck', 'brk1')], 'ASSERT-FAIL')]),
}
for _component, _key in (('nats', '39.NI'), ('caddy', '39.CI')):
    ARMS[_key] = dict(model=functools.partial(Model, _component, 'single', 'legacy-capability', binary='/opt/sim/tether-next'),
                      faults=[rule('sim', '*admin infra upgrade*', 64)], body=legacy_body, legacy=True, negatives=[
        ('historical target accepted the infra upgrade rc 0', [rule('sim', '*admin infra upgrade*', 0)], 'ASSERT-FAIL'),
        ('target is not the genuine tether-prev', [knob('node.legacy_sha')], 'ASSERT-FAIL', [('cli',)]),
        ('tether-prev release source unproven', [rule('artifacts', 'verify *', 1)], 'SETUP-RED', [('fixture', 'preflight')]),
        ('refusal re-executed the old broker', [knob('node.broker_reexec', 'brk1')], 'ASSERT-FAIL', [('fixture', 'finish')]),
        ('old target advertises infra capability', [knob('node.legacy_capable')], 'ASSERT-FAIL', [('cli',)])])
# Every arm: the live path of today (no composer supply) must fail closed.
GENERIC = [('live requiredset composer binding absent (P19/P30 pending)', [('nobinding', None)], 'SETUP-RED',
            [('fixture', 'prepare'), ('sim', 'nuke')])]
# FINAL settlement, log windows and cleanup receipts are one arm-independent
# library path; each runs a full real schedule, so they are aimed at one arm of
# every shape (NATS cycle, Caddy hermetic TLS, no-roll exercise, legacy) rather
# than multiplying full schedules on a shared host by nineteen.
SETTLEMENT = [('final FIN/native settlement red', [knob('final.fin_missing', 'ssh')], 'ASSERT-FAIL', [('fixture', 'check', 'logs')]),
              ('final log stream unreadable', [rule('logs', 'agt2 agent_panic', 1, 2)], 'SETUP-RED', [('fixture', 'check', 'logs')]),
              ('panic inside the fresh broker log window', [rule('logs', 'brk1 broker_panic', 0, 2, 'panic')], 'ASSERT-FAIL'),
              ('composer keeper exits nonzero after settling', [knob('keeper.stop_rc', 3)], 'SETUP-RED')]
for _key in ('85.A', '86.R', '85.S', '39.NI'):
    ARMS[_key]['negatives'] = ARMS[_key]['negatives'] + SETTLEMENT
# The library verb (and check scope) whose native status must be the FIRST
# nonzero one: a fault must turn red where it is aimed, not at an earlier
# guard that happens to see the same perturbation.
ROUND, REFUSAL, HALTED = ('check', 'round'), ('check', 'refusal'), ('check', 'halted')
RED = {'rollback CLI rc 75 is not a completed roll': ROUND, 'old incarnation alive after the reupgrade hop': ROUND,
       'management bootstrap leaves the stock unit unmanaged': ('check', 'bootstrap'), STUCK: ROUND,
       'cluster upgrade rc 75 on the reupgrade round': ROUND, 'rollback hop never moves the attachment': ROUND,
       'an original transfer settles before the first cut': ROUND, 'N2 refusal restarted a server': REFUSAL,
       'product shrink leaves a pair': ('heal',), 'post-heal sole-voter roll rc 75': ROUND,
       'lost-quorum fault cannot be installed': ('fault',), 'lost-quorum instrument not active at the refusal': REFUSAL,
       'unsafe cut accepted rc 0': REFUSAL, 'consumer-lag cut accepted rc 0': REFUSAL, 'stream-lag heal fails': ('heal',),
       'final roll rc 75 after both lag refusals': ROUND, 'second launch exercise cannot run': ('exercise',),
       'second launch opened the store': ('check', 'store-lock'), 'legacy owner guard never attempted': ('check', 'store-lock'),
       'join exercise cannot run': ('exercise',), 'mismatched release joined': ('check', 'join'),
       'temporary broker never retired': ('check', 'join'), 'halted output names no transaction': HALTED,
       'reply cut never hit the sent request': HALTED, 'product status receipt unavailable (P24 pending)': ('transaction',),
       'product recover fails': ('check', 'recovered'), 'valid reload refused rc 64': ('check', 'reload'),
       'reloaded generation is not the staged digest': ('check', 'reload'), 'configuration input cannot be staged': ('stage-config',),
       'old Caddy alive after the first hop': ROUND, 'upgrade round rc 75': ROUND,
       'ACME late-write fault cannot be installed': ('fault',), 'retrofit bootstrap leaves the stock unit unmanaged': ('check', 'bootstrap'),
       'first hop never moves the attachment': ROUND, 'second-node bootstrap rc 1': ('check', 'bootstrap'),
       'invalid config accepted rc 0': ('check', 'invalid-config-cert'),
       'refused input still changed the generation': ('check', 'invalid-config-cert'),
       'invalid certificate cannot be staged': ('stage-config',), 'responsibility exercise cannot run': ('exercise',),
       'external storage gate never exercised': ('check', 'responsibility-gates'),
       'live QUIC force-closed and signed': ('check', 'responsibility-gates'), 'controller death never hit': HALTED,
       'shadow stopped serving past the deadline': HALTED, 'product status says the halted transaction completed': ('transaction',),
       'rollback-support helper rc 1': ('check', 'support'), 'rollback-support never moves the attachment': ('check', 'support'),
       'historical target accepted the infra upgrade rc 0': ('check', 'legacy-refusal'),
       'target is not the genuine tether-prev': ('check', 'legacy-source'),
       'refusal re-executed the old broker': ('check', 'legacy-refusal'), 'final FIN/native settlement red': ('finish',),
       'live requiredset composer binding absent (P19/P30 pending)': ('preflight',),
       'panic inside the fresh broker log window': ('check', 'logs'),
       'composer keeper exits nonzero after settling': ('closed',),
       'stock owner still in the kernel census at retirement': ('check', 'retirement'),
       'deferral output names no closed refusal code': REFUSAL, 'halt let a hop retire before recovery': HALTED,
       'reload replaced the Caddy process': ('check', 'reload'),
       'retrofit bootstrap restarted the stock server': ('check', 'bootstrap'),
       'old target advertises infra capability': ('check', 'legacy-source')}


class Run:
    pass


def run_arm(key, faults, supervisor=None):
    """One fresh sandbox, one real drill process for one arm; returns its native record."""
    number, arm = key.split('.', 1)
    with tempfile.TemporaryDirectory(prefix='r9d-infra-' + key + '.') as temporary:
        base = Path(temporary)
        loopback = sandbox.Loopback(base) if key in HERMETIC else None
        try:
            root, fake, poison = sandbox.build(base, faults, loopback)
            env = dict(PATH=str(poison) + ':/usr/bin:/bin', HOME=str(base / 'home'), TMPDIR=str(base / 'tmp'), LANG='C',
                       LC_ALL='C', HERE=str(root), ARM=arm, INSTANCE='drill-r9d-infra-' + key.replace('.', '-'),
                       SIM_EVIDENCE_DIR=str(root / 'evidence'), INFRA_ARM_FAKE=str(fake),
                       INFRA_ARM_DOUBLE=str(sandbox.DOUBLE), DOCKER=str(fake / 'docker'), SIM_DRILL_NAME=DRILLS[number],
                       TX32=TX32, SIM_UPGRADE_SUPERVISOR=os.environ['SIM_UPGRADE_SUPERVISOR'] if supervisor is None else supervisor)
            result = subprocess.run(['/bin/sh', str(root / 'drills' / (DRILLS[number] + '.sh'))], env=env, cwd=str(base),
                                    stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=300)
        finally:
            if loopback:
                loopback.close()
        record = Run()
        record.rc, record.stderr, record.root = result.returncode, result.stderr, str(root)
        record.evidence = sorted(str(p) for p in (root / 'evidence').glob('hot-update-infra.*'))
        record.calls = (fake / 'calls').read_text() if (fake / 'calls').exists() else ''
        record.hits = {p.name[4:] for p in fake.glob('hit.*')}
        record.ledger, record.steps, record.final, record.observer = [], {}, None, None
        for directory in map(Path, record.evidence):
            if (directory / 'fixture/verbs.jsonl').exists():
                record.ledger = [json.loads(line) for line in (directory / 'fixture/verbs.jsonl').read_text().splitlines()]
            if (directory / 'fixture/final.json').exists():
                record.final = json.loads((directory / 'fixture/final.json').read_text())
            for path in directory.glob('step-*.log'):
                rc = path.with_suffix('.rc')
                record.steps[path.name] = (path.read_text(), rc.read_text().strip() if rc.exists() else None)
            # The scheduler's own closed custody code, for a red FINAL's diagnosis.
            if (directory / 'fixture.out').exists():
                record.observer = (directory / 'fixture.out').read_text()[-400:]
        record.shas = {sandbox.sha(root / 'vendor' / (b + '-next')) for b, _, _ in VERSIONS.values()}
        knobs = json.loads((fake / 'knobs.json').read_text())
        record.configs = {sandbox.hashlib.sha256(('config-' + v).encode()).hexdigest()
                          for v in ('valid', 'invalid-config', 'invalid-cert')} | {knobs.get('config.sha', '')}
        return record


def events(record):
    """Merge library verbs, $SIM/$DOCKER calls and composer instruments by wall clock."""
    rows, poisoned, trace = [], [], []
    for line in record.calls.splitlines():
        at, tool, *args = line.split('\x1f')
        rows.append((int(at), tool, args))
    rows += [(row['at_ns'], 'fixture', [row['verb']] + row['argv']) for row in record.ledger]

    def norm(value):
        for directory in record.evidence:
            if value == directory:
                return 'EV'
            if value.startswith(directory + '/step-') and value.endswith('.log'):
                return 'LOG'
        if value in record.shas:
            return 'SHA'
        if value in record.configs:
            return 'CFG'
        return {TX32: 'TX', record.root + '/simcluster': 'SIM'}.get(value, value.replace(record.root, 'HERE'))

    for _, tool, args in sorted(rows, key=lambda r: r[0]):
        args = [norm(a) for a in args]
        if tool == 'poison':
            poisoned.append(tuple(args))
        elif tool == 'sim' and args[:1] == ['status']:
            continue
        elif tool == 'sim' and args[:1] == ['exec'] and len(args) > 3 and args[2] == '--':
            node, command = args[1], args[3:]
            if command[:1] == ['curl'] or command[:4] == ['tether', 'cluster', 'status', '--json']:
                continue
            if command[:len(CLI_PREFIX)] == CLI_PREFIX:
                trace.append(('cli', node) + tuple(command[len(CLI_PREFIX):]))
            elif command[:1] == ['/usr/local/libexec/tether-infra-helper']:
                trace.append(('helper', node) + tuple(command[1:]))
            else:
                trace.append(('sim',) + tuple(args))
        else:
            trace.append((tool,) + tuple(args))
    return trace, poisoned


def verdict(record):
    rows = VERDICT.findall(record.stderr)
    tail = '\n'.join(record.stderr.splitlines()[-25:])
    assert len(rows) == 1 and len(re.findall(r'^DRILL-VERDICT\b', record.stderr, re.M)) == 1, \
        'missing/duplicate/malformed DRILL-VERDICT (rc %s)\n%s' % (record.rc, tail)
    assert record.rc == int(rows[0][1]) == CODES[rows[0][0]], 'native rc %s translated from %s' % (record.rc, rows[0][0])
    return rows[0][0], rows[0], tail


def absent(trace, prefixes):
    return next(('event %r must not run, found %r' % (p, e) for p in prefixes for e in trace if e[:len(p)] == tuple(p)), None)


def check_manifest(key):
    number, arm = key.split('.', 1)
    arms = header_arms(DRILLS[number])
    if number != '39':
        assert sorted(arms) == sorted(k.split('.', 1)[1] for k in ARMS if k.startswith(number + '.')), arms
    assert arm in arms and expected_verdicts().get(unit_name(key)) in CODES, key
    # Every oracle-aimed negative names the verb that must turn red first.
    assert all(n[0] in RED for n in ARMS[key]['negatives'] + GENERIC if n[2] in ('ASSERT-FAIL', 'INCOMPLETE')), key


def check_positive(key):
    want = ARMS[key]['body'](ARMS[key]['model']())
    record = run_arm(key, ARMS[key].get('faults', []))
    name, row, tail = verdict(record)
    trace, poisoned = events(record)
    assert name == expected_verdicts()[unit_name(key)] and int(row[8]) > 0, '%s reached %s\n%s\nFINAL %r\n%s' % (
        key, name, tail, record.final, record.observer)
    assert not poisoned and len(record.evidence) == 1, (poisoned, record.evidence)
    first = next((i for i in range(max(len(trace), len(want))) if trace[i:i + 1] != want[i:i + 1]), None)
    assert first is None, 'trace differs from the %s obligation sequence at #%s:\n  got  %r\n  want %r' % (
        key, first, trace[first:first + 1], want[first:first + 1])
    # Every library verb, each scoped judge included, returned native 0, and
    # the FINAL probe signed this arm in its own scope (hermetic: Caddy hops).
    assert record.ledger and all(r['rc'] == 0 for r in record.ledger), [(r['verb'], r['rc']) for r in record.ledger]
    assert record.final and record.final['native_rc'] == 0 and record.final['evidence_scope'] == (
        'hermetic' if key in HERMETIC else 'live'), record.final
    forwarded = sorted(e[e.index('--command-rc') + 1] for e in want if '--command-rc' in e)
    assert sorted(rc for _, rc in record.steps.values() if rc != '0') == [rc for rc in forwarded if rc != '0'], record.steps


def check_unsupervised(key):
    # The shared FIX-ORPHAN owner precedes the whole shell: a relative adapter
    # path must stop the arm before any boundary call or library verb.
    record = run_arm(key, [], supervisor='supervise')
    touched = [line for line in record.calls.splitlines() if '\x1fpoison\x1fdocker\x1fps' not in line]
    assert record.rc != 0 and not touched and not record.ledger and 'GREEN' not in record.stderr, (record.rc, touched)


def check_negative(key, negative):
    description, faults, want = negative[:3]
    record = run_arm(key, list(faults) + list(ARMS[key].get('faults', [])))
    name, _, tail = verdict(record)
    trace, poisoned = events(record)
    assert name == want, '%s [%s] reached %s, want %s\n%s\nFINAL %r\n%s' % (
        key, description, name, want, tail, record.final, record.observer)
    rules = [value for kind, value in faults if kind == 'rule']
    for index in range(1, len(rules) + 1):
        assert str(index) in record.hits, '%s [%s] injected rule #%d never reached' % (key, description, index)
    for kind, value in faults:
        assert kind != 'knob' or value[0] in record.hits, '%s [%s] knob %s never consulted' % (key, description, value[0])
        if kind == 'nobinding':
            # The live path today: the real library names its pending owner.
            assert any('NOT_RUN pending S9 P19/P30' in text for text, _ in record.steps.values()), record.steps
    failed = next(((r['verb'],) + ((r['argv'][0],) if r['verb'] == 'check' else ()) for r in record.ledger if r['rc']), None)
    assert description not in RED or failed == RED[description], '%s [%s] first red verb %r, want %r' % (
        key, description, failed, RED[description])
    problem = absent(trace, negative[3] if len(negative) > 3 else [])
    assert problem is None and not [e for e in trace if e[0] == 'unexpected'], '%s [%s]: %s' % (key, description, problem)
    assert all(p[:2] == ('docker', 'ps') for p in poisoned), poisoned


def case(name, call, description):
    def test():
        call()
    test.__name__ = name
    return unittest.FunctionTestCase(test, description=description)


def arm_suite(key):
    suite = unittest.TestSuite()
    suite.addTest(case('test_declared_arm', functools.partial(check_manifest, key), key + ' declared arm and expected verdict row'))
    suite.addTest(case('test_positive', functools.partial(check_positive, key),
                       key + ' real drill and real library reach the expected verdict with the exact obligation trace'))
    suite.addTest(case('test_unsupervised', functools.partial(check_unsupervised, key), key + ' refuses without the shared supervisor'))
    for index, negative in enumerate(ARMS[key]['negatives'] + GENERIC):
        suite.addTest(case('test_negative_%02d' % index, functools.partial(check_negative, key, negative),
                           '%s %s -> %s' % (key, negative[0], negative[2])))
    return suite


def fan_out(keys, results, jobs):
    """One isolated interpreter per arm, each leaving its own native receipt."""
    from concurrent.futures import ThreadPoolExecutor
    results.mkdir(parents=True, exist_ok=False)

    def one(key):
        with open(results / (key + '.out'), 'w') as log:
            try:
                rc = subprocess.run([sys.executable, '-I', '-B', str(Path(__file__).resolve()), '--arm', key],
                                    stdout=log, stderr=subprocess.STDOUT, timeout=1200).returncode
            except subprocess.TimeoutExpired:
                rc = 124
        (results / (key + '.rc')).write_text(str(rc) + '\n')
        return rc

    with ThreadPoolExecutor(max_workers=jobs) as pool:
        codes = list(pool.map(one, keys))
    return 0 if codes and all(code == 0 for code in codes) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--arm', required=True)
    parser.add_argument('--results', type=Path, help='fan out one interpreter per arm; receipts land here')
    # Each arm runs a real 10/s schedule against a 5s budget; bound the arms in
    # flight so a shared host cannot turn this control into a timing flake.
    parser.add_argument('--jobs', type=int, default=max(2, min(12, (os.cpu_count() or 2) // 4)))
    args = parser.parse_args()
    if args.arm != 'all' and args.arm not in ARMS:
        # An unknown selector must not run zero tests and pass vacuously.
        print('unknown infra arm selector: ' + args.arm, file=sys.stderr)
        return 2
    supervisor = os.environ.get('SIM_UPGRADE_SUPERVISOR', '')
    if not (supervisor.startswith('/') and os.access(supervisor, os.X_OK)):
        print('infra arm controls: the shared FIX-ORPHAN supervisor (SIM_UPGRADE_SUPERVISOR) is required', file=sys.stderr)
        return 2
    selected = list(ARMS) if args.arm == 'all' else [args.arm]
    if args.results:
        return fan_out(selected, args.results, max(1, args.jobs))
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(arm_suite(key) for key in selected))
    return 0 if result.wasSuccessful() and result.testsRun and not result.skipped else 1


if __name__ == '__main__':
    sys.exit(main())
