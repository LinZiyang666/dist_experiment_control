#!/usr/bin/env -S python3 -I -B
"""Real original flow, PTY and exec lifetimes behind the S9 owner ports.

Product/remote helper provisioning must precede prepare. This adapter never
installs a product or restarts an agent. The original byte ledgers and native
statuses remain evidence for the independent reader, not success receipts.
origin: hot-update S9 plan §9.1 / LQ3 / L22; S9 INSTRUMENTS-2.
"""
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import os
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace

import infra_composer_contract as contract
import hot_update_infra_workload as workload
from infra_owner_lifecycle import Job
from infra_owner_plan import LIB, need


def module(name):
    spec = importlib.util.spec_from_file_location(name, LIB / (name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def cli(plan, verb):
    product = plan['product']
    return [product['binary']['path'], *verb, '--home', product['home'], '--nats-url', product['nats_url']]


def run(plan, argv, timeout=10, allowed=(0,)):
    contract.file_pin(plan['supervisor'])
    contract.file_pin(plan['product']['binary'])
    rc, raw = workload.Supervisor(plan['supervisor']['path']).run(argv, timeout)
    need(rc in allowed, 'product', 'native_command_failed_' + str(rc))
    return raw


def remote(plan, *argv, timeout=10):
    product = plan['product']
    return run(plan, cli(plan, ['exec', '--timeout', str(timeout) + 's']) +
               [product['node'], '--', *map(str, argv)], timeout + 1)


def declare_flows(peer, specs, nonce, seconds):
    # Freeze a sufficient finite supply before original connect.
    # No producer is paused, replenished or slowed after a fault.
    # S5's 2400-record default is too short for the S9 arm window.
    from fullstack_flow import declare_source, source_records
    for spec in specs:
        if spec['id'] == 'tcp-1':
            spec['class'] = 'idle'
            for direction in ('up', 'down'):
                spec[direction] = peer.declaration(spec['id'] + '.' + direction, nonce, 0, spec['size'])
            continue
        if spec['class'] == 'half-close':
            count = source_records(seconds + 600, 4)
            spec['down'] = peer.declaration(spec['id'] + '.down', nonce, count, spec['size'])
        else:
            declaration = declare_source(peer, spec['id'], nonce, seconds + 600,
                size=spec['size'], protocol=spec['protocol'], burst=spec.get('burst', False))
            for key in ('up', 'down', 'supply'):
                spec[key] = declaration[key]


class Task:
    def __init__(self, plan, context, root, kind):
        self.plan, self.context, self.root, self.kind = plan, context, root, kind
        self.job, self.drained, self.closed = None, False, False
        self.observed_tasks = set()
        self.remote_root = str(Path(plan['product']['effect_root']) / (kind + '-' + context['nonce']))

    def start(self):
        row = self.plan['product']
        lifetime = self.context['seconds'] + 600
        if self.kind == 'exec':
            argv = cli(self.plan, ['exec', '--timeout', str(lifetime + 30) + 's']) + [row['node'], '--',
                row['remote_python'], '-I', '-B', self.plan['exec']['remote_probe'], 'producer',
                '--root', self.remote_root, '--nonce', self.context['nonce'], '--lifetime', str(lifetime)]
            expected = 23
        else:
            argv = [str(Path('/usr/bin/python3').resolve()), '-I', '-B', str(LIB / 'hot_update_pty_probe.py'),
                'trace', '--role', 'ctl', '--evidence', str(self.root), '--nonce', self.context['nonce'],
                '--ctl-binary', row['binary']['path'], '--timeout', str(lifetime), '--scenario', 'A', '--'] + \
                cli(self.plan, ['run'])[1:] + [row['node'], '--', row['remote_python'], '-I', '-B',
                self.plan['pty']['remote_probe'], 'trace', '--role', 'leader', '--scenario', 'A',
                '--evidence', self.remote_root, '--nonce', self.context['nonce'], '--timeout', str(lifetime)]
            expected = 0  # The trace retains the product's waitpid status separately.
        self.job = Job(self.root, argv, self.plan['supervisor']['path'], expected)
        # A running wrapper is not READY. Wait for actual original target
        # progress and retain its raw response; native collector binds task IDs.
        deadline = time.monotonic() + 20
        while True:
            self.job.intact()
            if self.kind == 'exec':
                try:
                    raw = remote(self.plan, row['remote_python'], '-I', '-B', self.plan['exec']['remote_probe'],
                                 'snapshot', '--root', self.remote_root, '--nonce', self.context['nonce'])
                    receipt = workload.decode(raw)
                    ready = receipt.get('alive') is True and receipt.get('data_count', 0) >= 20
                except ValueError:
                    ready = False
            else:
                target = self.root / 'ctl.trace.json'
                ready = target.exists() and (self.root / 'ctl.received').stat().st_size > 0
                raw = target.read_bytes() if ready else b''
            if ready:
                (self.root / 'original-ready.json').write_bytes(raw)
                return
            need(time.monotonic() < deadline, self.kind, 'original_progress_deadline')
            time.sleep(.05)

    def drain(self):
        if self.drained:
            return
        if self.kind == 'pty':
            (self.root / 'finish').touch(exist_ok=False)
        else:
            row = self.plan['product']
            remote(self.plan, row['remote_python'], '-I', '-B', self.plan['exec']['remote_probe'],
                   'stop', '--root', self.remote_root, '--nonce', self.context['nonce'])
        self.job.join(30)
        if self.kind == 'pty':
            # The trace wrapper itself exits zero after saving wait_status.
            # Require the original product's real exit 23 too.
            result = contract.read(self.root / 'ctl.wait.json')
            need(result.get('reaped') is True and os.waitstatus_to_exitcode(result['wait_status']) == 23,
                 'pty', 'original_native_status')
        self.drained = True

    def intact(self):
        if self.job is not None and not self.drained:
            self.job.intact()

    def snapshot(self):
        # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-3.
        # Capture the real ps token and original target, never an ID derived
        # from the owner wrapper PID. Sampling does not drain either channel.
        self.intact()
        row = self.plan['product']
        ps = workload.decode(run(self.plan, cli(self.plan, ['ps', '--json'])))
        need(not ps.get('procs_truncated', False), self.kind, 'truncated_task_census')
        tasks = [task for task in ps['processes'] if self.remote_root in task.get('argv', [])]
        need(len(tasks) == 1 and tasks[0].get('nid') == row['node'] and tasks[0].get('pid'),
             self.kind, 'original_task_identity')
        if self.kind == 'exec':
            target = workload.decode(remote(self.plan, row['remote_python'], '-I', '-B',
                self.plan['exec']['remote_probe'], 'snapshot', '--root', self.remote_root,
                '--nonce', self.context['nonce']))
            need(target['alive'] is True, self.kind, 'original_target_dead')
            ctl_pid = self.job.child.target_pid
        else:
            target = workload.decode(remote(self.plan, 'cat', self.remote_root + '/remote.identity.json'))
            ctl_pid = contract.read(self.root / 'ctl.identity.json')['identity']['pid']
        current = target.get('current', target.get('identity', {}))
        self.observed_tasks.add((tasks[0]['pid'], current.get('pid'), current.get('start_ticks'), ctl_pid))
        return dict(task=tasks[0], target=target, ctl_pid=ctl_pid, remote_root=self.remote_root,
                    accepts=len(tasks), redials=len(self.observed_tasks) - 1)

    def close(self):
        if self.closed:
            return
        self.closed = True
        if self.job is not None:
            self.job.close()


class Flows:
    def __init__(self, plan, context, root):
        self.plan, self.context, self.root = plan, context, root
        self.fixture, self.drained, self.closed = None, False, False

    def start(self):
        wrapper = module('hot_update_flow_fixture')
        impl = wrapper.implementation()
        seconds = self.context['seconds']
        class InfraFlow(impl.Fixture):
            def remote_call(self, node, command, **fields):
                if command == 'configure':
                    declare_flows(impl.peer, fields['flows'], self.nonce, seconds)
                return super().remote_call(node, command, **fields)

            def baseline(self):
                deadline = time.monotonic() + 20
                while True:
                    snapshots = [self.remote_call(node, 'snapshot') for node in ('ctl1', 'agt1')]
                    ready = all(spec['id'] in doc['flows'] and (
                        not doc['flows'][spec['id']]['sent'] and not doc['flows'][spec['id']]['received']
                        if spec['class'] == 'idle' else
                        len(doc['flows'][spec['id']]['sent']) >= 20 and len(doc['flows'][spec['id']]['received']) >= 20)
                        for doc in snapshots for spec in self.specs if spec['id'] != 'opening')
                    if ready:
                        return self.checkpoint('infra-baseline')
                    need(time.monotonic() < deadline, 'flow', 'original_progress_deadline')
                    time.sleep(.05)

        class InfraTransport(impl.Transport):
            def start(self, stdout, stderr, *arguments):
                values = list(arguments)
                if 'serve' in values and any(str(v).endswith('/flow_fixture_peer.py') for v in values):
                    node = values[1]
                    index = next(i for i, v in enumerate(values) if str(v).endswith('/flow_fixture_peer.py'))
                    destination = str(Path(values[index]).with_name('infra_flow_peer.py'))
                    self.run('copy-to', LIB / 'infra_flow_peer.py', node, destination)
                    self.run('copy-to', LIB / 'fullstack_flow.py', node, str(Path(destination).with_name('fullstack_flow.py')))
                    values[index:index + 1] = [destination]
                return super().start(stdout, stderr, *values)

        impl.PROFILES += ('broker',)
        flow = self.plan['flow']
        args = SimpleNamespace(root=self.root, nonce=self.context['nonce'], profile='broker', arm='A',
            here=Path(flow['here']), sim=flow['sim']['path'], instance=self.context['instance'],
            request_scheduler=False)
        self.fixture = InfraFlow(args, InfraTransport(args))
        self.fixture.prepare()
        # S10's peer admission seals supplied declarations on both ends. The
        # historical S5 client omitted configure because it had no supply key.
        self.fixture.remote_call('ctl1', 'configure', flows=self.fixture.specs)
        self.fixture.start()
        self.fixture.baseline()

    def drain(self):
        if self.drained:
            return
        fixture = self.fixture
        need(fixture is not None and fixture.phase == 'running', 'flow', 'drain_before_start')
        with ThreadPoolExecutor(max_workers=2) as workers:
            results = [workers.submit(fixture.remote_call, node, 'finish') for node in ('ctl1', 'agt1')]
            for result in results:
                receipt = result.result()
                need(receipt.get('rc', 0) == 0, 'flow', 'native_finish_failed')
        fixture.checkpoint('final')
        for node, role in (('agt1', 'target'), ('ctl1', 'client')):
            folder = self.root / role
            folder.mkdir(mode=0o700, exist_ok=True)
            for spec in fixture.specs:
                if spec['id'] in fixture.snapshots['final'][role]['flows']:
                    for suffix in ('sent', 'received', 'json'):
                        name = spec['id'] + '.' + suffix
                        fixture.transport.run('copy-from', node, fixture.remote + '/' + name, folder / name)
        # The independent collector consumes these originals and the log API.
        # This port does not manufacture an S5 evidence.json or oracle verdict.
        fixture.phase = 'finished'
        self.drained = True

    def intact(self):
        if self.fixture is not None:
            need(all(job.poll() is None for job in self.fixture.jobs), 'flow', 'original_peer_exited')

    def snapshot(self):
        # Keep the snapshot in the original foreground owner: a fresh fixture
        # would create new sockets and erase accepts/redials from the evidence.
        # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-3.
        self.intact()
        need(self.fixture is not None, 'flow', 'owner_not_started')
        fixture = self.fixture
        with ThreadPoolExecutor(max_workers=2) as workers:
            reads = [workers.submit(fixture.remote_call, node, 'snapshot') for node in ('ctl1', 'agt1')]
            client, target = [read.result() for read in reads]
        bindings = fixture.remote_call('ctl1', 'infra-bindings', target=target)
        return dict(client=client, target=target, remote_root=fixture.remote, bindings=bindings)

    def close(self):
        if self.closed:
            return
        self.closed = True
        if self.fixture is not None:
            self.fixture.cleanup()
