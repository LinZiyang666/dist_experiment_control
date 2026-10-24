#!/usr/bin/env -S python3 -I -B
"""Foreground S9 required-set composer; every owner enters FIX-ORPHAN.

keeper/control implement P32's frozen cohort protocol. instrument execs the
pinned deployment reader/client in place, preserving the production attempt
sink descriptor. The composer never repairs a product, pauses a transfer,
retries a workload, or manufactures a process/socket/transfer observation.

Supply is staged by lib/infra-composer.py. A live supply must come from the
actual deployment owners and include the lead-accepted S7 handoff; absent
owners are setup errors. Hermetic supply remains labelled hermetic throughout.
origin: hot-update S9 plan §9.1 / §9.4 / L22 / P19/P26/P30; S9 COMPOSER.
"""
import argparse
import contextlib
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import infra_composer_contract as contract
import hot_update_infra_probe as probe
import hot_update_infra_verbs as verbs
import hot_update_infra_workload as workload


@contextlib.contextmanager
def address(root):
    # Long evidence paths must not consume sockaddr_un's 108 byte limit.
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        yield '/proc/self/fd/' + str(fd) + '/composer.sock'
    finally:
        os.close(fd)


def private_root(value):
    root = Path(value)
    contract.require(root.is_absolute() and root.is_dir() and not root.is_symlink(), 'evidence_path')
    info = root.stat()
    contract.require(info.st_uid == os.geteuid() and info.st_mode & 0o077 == 0, 'evidence_custody')
    return root


def session_path(supply):
    instance = os.environ.get('INSTANCE', '')
    contract.require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}', instance) is not None, 'instance_required')
    base = Path(os.environ.get('SIM_INFRA_COMPOSER_RUNS', '/tmp/tether-infra-composer-' + str(os.geteuid())))
    base.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_root(base)
    key = contract.hashlib.sha256((str(Path(supply).resolve()) + '\n' + instance).encode()).hexdigest()
    return base / (key + '.json')


def birth(pid):
    row = (Path('/proc') / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()
    contract.require(row[0] not in ('Z', 'X'), 'owner_dead')
    return int(row[19])


def active(args, *, control=False):
    supply = contract.load(args.supply, args.supply_sha256)
    session = contract.read(session_path(args.supply))
    contract.require(session['supply_sha256'] == args.supply_sha256
                     and birth(session['pid']) == session['start_ticks'], 'stale_session')
    root = private_root(session['evidence'])
    context = contract.read(root / 'context.json')
    contract.require(contract.sha(root / 'context.json') == session['context_sha256'], 'context_changed')
    state = contract.read(root / 'state.json')
    contract.require(state['phase'] in ('prepared', 'running') and not (root / 'failure.json').exists(), 'owner_failed')
    if not control:
        contract.require(state['phase'] == 'running' or args.table == 'readers' and args.name == 'node', 'not_ready')
    return supply, root, context


def request(root, verb):
    private_root(root)
    with address(root) as endpoint, socket.socket(socket.AF_UNIX) as channel:
        channel.settimeout(620)
        channel.connect(endpoint)
        channel.sendall(contract.canonical(dict(verb=verb)))
        channel.shutdown(socket.SHUT_WR)
        raw = bytearray()
        while True:
            part = channel.recv(65536)
            if not part:
                break
            raw.extend(part)
            contract.require(len(raw) <= 1 << 20, 'control_overflow')
    result = workload.decode(raw)
    contract.require(set(result) == {'native_rc'} and result['native_rc'] == 0, 'control_failed')


def run(spec, arguments, timeout, root, label):
    # Retain each raw owner response independently of the composed READY.
    rc, raw = workload.Supervisor(os.environ.get('SIM_UPGRADE_SUPERVISOR', '')).run(
        workload.command(contract.spec(spec)) + arguments, timeout)
    path = root / (label + '.out')
    with path.open('xb') as output:
        output.write(raw)
    (root / (label + '.rc')).write_text(str(rc) + '\n')
    contract.require(rc == 0, 'owner_command_failed')
    return raw


class Keeper:
    def __init__(self, args):
        self.args = args
        self.supply = contract.load(args.supply, args.supply_sha256)
        self.root = private_root(args.evidence)
        self.profile = self.supply['profiles'][':'.join((args.component, args.topology, args.profile))]
        self.jobs, self.phase = {}, 'preparing'
        self.session, self.registered = session_path(args.supply), False
        self.supervisor = workload.Supervisor(os.environ.get('SIM_UPGRADE_SUPERVISOR', ''))
        contract.require(args.nodes.split() == list(verbs.TOPOLOGY[args.topology])
                         and args.agents == 'agt1,agt2' and args.ctl == 'ctl1'
                         and probe.digest(args.initial_sha256), 'arm_binding')
        self.context = dict(schema='infra-composer-context-v1', mode=self.supply['mode'], component=args.component,
            topology=args.topology, profile=args.profile, nodes=args.nodes.split(), agents=args.agents.split(','),
            ctl=args.ctl, initial_sha256=args.initial_sha256, seconds=self.profile['seconds'],
            rates=self.profile['rates'], nonce=secrets.token_hex(16), evidence=str(self.root),
            instance=os.environ['INSTANCE'], hops=verbs.schedule(vars(args)))
        self.context_file = self.root / 'context.json'
        contract.require(not self.context_file.exists(), 'fresh_cohort_required')
        workload.publish(self.context_file, self.context)
        self.context_sha = contract.sha(self.context_file)
        self.arguments = ['--context', str(self.context_file)]

    def intact(self):
        # Recheck supply and context at every spawn/control boundary. Binding
        # pins alone do not protect a keeper that was started minutes earlier.
        contract.require(contract.sha(self.args.supply) == self.args.supply_sha256
                         and contract.sha(self.context_file) == self.context_sha, 'supply_or_context_changed')
        for name, job in self.jobs.items():
            contract.require(job.poll() is None, 'required_owner_exited_' + name)

    def save(self, phase):
        self.phase = phase
        workload.publish(self.root / 'state.json', dict(phase=phase, pid=os.getpid(), jobs={
            name: job.pid for name, job in self.jobs.items()}))

    def register(self):
        session = dict(pid=os.getpid(), start_ticks=birth(os.getpid()), evidence=str(self.root),
                       context_sha256=self.context_sha, supply_sha256=self.args.supply_sha256)
        # No stale-session takeover: the lead retains the failed run and removes
        # its pointer explicitly. Concurrent runs require distinct INSTANCEs.
        with self.session.open('xb') as output:
            os.chmod(self.session, 0o600)
            output.write(contract.canonical(session))
        self.registered = True

    def prepare(self):
        run(self.profile['prepare'], self.arguments, 600, self.root, 'prepare')
        self.intact()
        self.save('prepared')
        self.register()
        workload.publish(self.root / 'prepared.json', dict(schema='infra-cohort-v1', phase='prepared', producer='composer',
            component=self.args.component, topology=self.args.topology, profile=self.args.profile,
            initial_sha256=self.args.initial_sha256))

    def launch(self):
        contract.require(self.phase == 'prepared', 'launch_once')
        self.intact()
        self.save('launching')
        for name in contract.OWNERS:
            root = self.root / name
            root.mkdir(mode=0o700)
            spec = contract.spec(self.profile['workloads'][name]['keeper'])
            with (root / 'owner.out').open('xb') as output:
                job = subprocess.Popen([str(self.supervisor.path), '--'] + workload.command(spec) +
                    self.arguments + ['--evidence', str(root)], stdout=output, stderr=subprocess.DEVNULL)
            self.jobs[name] = job
        raw = run(self.profile['ready'], self.arguments, 600, self.root, 'ready-reader')
        self.intact()
        value = contract.ready(workload.decode(raw), self.context, self.supply)
        self.intact()
        self.save('running')
        workload.publish(self.root / 'ready.json', value)

    def close(self):
        failed = False
        # Every started owner is stopped even after partial launch or a failed
        # ready reader. Success requires native zero and an actual joined job.
        for name, job in self.jobs.items():
            if job.poll() is None:
                try:
                    run(self.profile['workloads'][name]['stop'], self.arguments + ['--evidence', str(self.root / name)],
                        20, self.root / name, 'stop')
                except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError):
                    failed = True
            else:
                failed = True
        deadline = time.monotonic() + 20
        for name, job in self.jobs.items():
            try:
                rc = job.wait(timeout=max(.001, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                failed = True
                job.terminate()
                try:
                    rc = job.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    job.kill()
                    rc = job.wait(timeout=5)
            (self.root / name / 'owner.rc').write_text(str(rc) + '\n')
            failed = failed or rc != 0
        self.save('failed' if failed else 'stopped')
        if self.registered:
            self.session.unlink()
            self.registered = False
        contract.require(not failed, 'owner_join_failed')

    def serve(self):
        failed = False
        try:
            with address(self.root) as endpoint, socket.socket(socket.AF_UNIX) as server:
                server.bind(endpoint)
                os.chmod(self.root / 'composer.sock', 0o600)
                server.listen(8)
                server.settimeout(.1)
                self.prepare()
                while True:
                    self.intact()
                    try:
                        channel, _ = server.accept()
                    except socket.timeout:
                        continue
                    with channel:
                        channel.settimeout(2)
                        raw = channel.recv(4096)
                        command = workload.decode(raw)
                        contract.require(command in ({'verb': 'launch'}, {'verb': 'stop'}), 'control_verb')
                        if command['verb'] == 'launch':
                            self.launch()
                        else:
                            self.close()
                        channel.sendall(contract.canonical(dict(native_rc=0)))
                        if command['verb'] == 'stop':
                            return 0
        except BaseException:
            failed = True
            workload.publish(self.root / 'failure.json', dict(native_rc=2, code='composer_owner_failed'))
            raise
        finally:
            if self.phase not in ('stopped', 'failed'):
                try:
                    self.close()
                except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError):
                    if not failed:
                        raise
            (self.root / 'composer.sock').unlink(missing_ok=True)


def instrument(args):
    supply, root, context = active(args)
    table = supply[args.table]
    spec = table[args.name]
    if args.table == 'faults':
        contract.require(bool(args.arguments) and args.arguments[0] in ('install', 'status', 'heal'), 'fault_action')
        spec = spec[args.arguments.pop(0)]
    if args.table == 'tls':
        spec = spec['reader']
    logical = None
    if args.table == 'commands':
        contract.require(len(args.arguments) == 1, 'logical_id_required')
        logical = args.arguments.pop()
    argv = workload.command(contract.spec(spec), logical) + ['--context', str(root / 'context.json')] + args.arguments
    # exec preserves the original native status and attempt sink FD. No extra
    # supervisor here: P32 has already put this exact process under FIX-ORPHAN.
    os.execv(argv[0], argv)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--supply', required=True)
    parser.add_argument('--supply-sha256', required=True)
    sub = parser.add_subparsers(dest='verb', required=True)
    keeper = sub.add_parser('keeper')
    for key in ('evidence', 'component', 'topology', 'profile', 'nodes', 'agents', 'ctl', 'initial-sha256'):
        keeper.add_argument('--' + key, required=True)
    control = sub.add_parser('control')
    control.add_argument('--evidence', required=True)
    control.add_argument('action', choices=('launch', 'stop'))
    instrument_parser = sub.add_parser('instrument')
    instrument_parser.add_argument('table', choices=('commands', 'readers', 'tls', 'faults', 'exercises', 'configs'))
    instrument_parser.add_argument('name')
    instrument_parser.add_argument('arguments', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        contract.load(args.supply, args.supply_sha256)
        if args.verb == 'keeper':
            def stopped(_number, _frame):
                raise probe.EvidenceError('composer_interrupted')
            signal.signal(signal.SIGTERM, stopped)
            signal.signal(signal.SIGINT, stopped)
            return Keeper(args).serve()
        if args.verb == 'control':
            _, root, _ = active(args, control=True)
            contract.require(root == Path(args.evidence), 'foreign_control_evidence')
            request(Path(args.evidence), args.action)
        else:
            instrument(args)
        return 0
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError):
        # Arbitrary deployment stdout/argv may contain private material. Error
        # classification is fixed; raw public receipts stay in private evidence.
        print('infra composer: SETUP-RED missing, changed or failed required owner', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
