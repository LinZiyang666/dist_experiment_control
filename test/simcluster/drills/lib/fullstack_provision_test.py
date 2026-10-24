#!/usr/bin/env -S python3 -I -B
"""Hermetic P11 controls. Synthetic roster rows never certify a deployment."""
import copy
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import socket
import ssl
import shutil
import pwd
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

LIB = Path(__file__).resolve().parent
sys.path.insert(0, str(LIB))
import fullstack_provision as provision
import flow_observer_service as observer
import fullstack_logs as logs


def manifest():
    artifact = dict(id='T0', role='agent', release='v0.0.0-T0', source_commit='1' * 40,
                    toolchain='go1.26.8', image_id='test-image', epoch=1, read_epochs=[1],
                    domains=['exec', 'pty', 'flow', 'ss', 'transfer'])
    artifact.update({field: 'a' * 64 for field in ('tree', 'dirty', 'flags', 'archive', 'binary', 'vendor', 'copy_manifest')})
    return dict(version=1, nonce='b' * 32, topology='single', runtimes=provision.runtime_rows(), artifact=artifact,
                supply=dict(path='/fixture/P10.json', sha='c' * 64, bytes=1, schema='P10-supply'),
                broker_url='wss://brk1:443/nats', tunnel_addr='brk1:7000')


def observations():
    result, online = [], []
    for i, row in enumerate(provision.runtime_rows()):
        result.append(dict(runtime=row['runtime'], key=dict(role='agent', uid=1400 + i,
                           **{k: row[k] for k in ('host', 'unit', 'sid', 'nid', 'domain')}),
                           home=row['home'], ctl_home=row['ctl_home'], process=dict(pid=100 + i, start_ticks=500 + i,
                           boot_id='fixture-boot', pidns='pid:[123]'), invocation_id='invocation-' + str(i),
                           cgroup='/system.slice/' + row['unit'], custody='/fixture/' + row['runtime'] + '/' + row['runtime'] + '.sock',
                           running_sha='a' * 64,
                           binary=dict(path=row['binary'], device=1, inode=1000 + i, sha='a' * 64, uid=1400 + i, mode=0o700),
                           config=dict(path=row['config'], device=1, inode=2000 + i, sha='d' * 64, uid=1400 + i, mode=0o600)))
        online.append(dict(sid=row['sid'], nid=row['nid'], status='ONLINE'))
    return result, online


# origin: hot-update S10 plan §3.1 / §5.3 / §10.2 P11
class FullstackProvisionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='s10-provision-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.document = manifest()
        self.rows, self.online = observations()

    def ready(self):
        return provision.ready_roster(self.document, self.rows, self.online)

    def test_ready_requires_four_real_sid_unit_domain_mappings(self):
        ready = self.ready()
        self.assertEqual(len(ready['required']), 4)
        self.assertEqual({row['sid'] for row in ready['required']},
                         {'s10-small-push', 's10-small-pull', 's10-large-push', 's10-large-pull'})
        self.assertEqual({row['host'] for row in ready['required']}, {'agt1', 'agt2'})
        for field in ('sid', 'nid', 'host', 'unit', 'user', 'home', 'domain', 'binary', 'ctl_home', 'config'):
            with self.subTest(field=field):
                document = manifest()
                document['runtimes'][1][field] = document['runtimes'][0][field]
                if field == 'host':
                    document['runtimes'][1][field] = 'agt2'
                with self.assertRaises(provision.ProvisionError):
                    provision.validate_manifest(document)
        for count in (0, 2, 3):
            with self.assertRaisesRegex(provision.ProvisionError, 'every runtime'):
                provision.ready_roster(self.document, self.rows[:count], self.online)

    def test_two_sessions_and_two_processes_cannot_impersonate_four(self):
        for index in (2, 3):
            self.online[index]['sid'] = self.online[index - 2]['sid']
        with self.assertRaisesRegex(provision.ProvisionError, 'independently registered'):
            self.ready()
        self.rows, self.online = observations()
        self.rows[1]['process'] = copy.deepcopy(self.rows[0]['process'])
        with self.assertRaisesRegex(provision.ProvisionError, 'runtime process'):
            self.ready()

    def test_ready_rejects_shared_binary_and_config_inodes(self):
        for kind in ('binary', 'config'):
            self.rows, self.online = observations()
            self.rows[1][kind]['inode'] = self.rows[0][kind]['inode']
            with self.subTest(kind=kind), self.assertRaisesRegex(provision.ProvisionError, 'pathname or inode'):
                self.ready()
        self.rows, self.online = observations()
        self.rows[1]['binary']['path'] = self.rows[0]['binary']['path']
        with self.assertRaisesRegex(provision.ProvisionError, 'pathname or inode'):
            self.ready()

    def test_ready_rejects_wrong_running_image_user_and_observer(self):
        changes = [('running_sha', 'e' * 64), ('custody', ''), ('invocation_id', ''), ('cgroup', ''), ('ctl_home', '/shared')]
        for field, value in changes:
            with self.subTest(field=field):
                self.rows, self.online = observations()
                self.rows[0][field] = value
                with self.assertRaises(provision.ProvisionError):
                    self.ready()
        self.rows, self.online = observations()
        self.rows[1]['key']['uid'] = self.rows[0]['key']['uid']
        for kind in ('binary', 'config'):
            self.rows[1][kind]['uid'] = self.rows[0]['key']['uid']
        with self.assertRaisesRegex(provision.ProvisionError, 'shared runtime account'):
            self.ready()

    def test_ready_prevents_restart_chown_and_repeated_start(self):
        fence = provision.SetupFence(self.root, self.document['nonce'])
        fence.seal(self.ready())
        with self.assertRaisesRegex(provision.ProvisionError, 'after READY'):
            fence.before_effect()
        commands = mock.Mock()
        host = provision.HostProvision(self.document, 'agt1', self.root, commands)
        for effect in (lambda: host.prepare('/missing-binary', '/observer', '/decoder'), host.start):
            with mock.patch.object(provision.os, 'chown') as chown:
                with self.assertRaisesRegex(provision.ProvisionError, 'after READY'):
                    effect()
                chown.assert_not_called()
        commands.run.assert_not_called()
        with self.assertRaisesRegex(provision.ProvisionError, 'after READY'):
            provision.SetupFence(self.root, self.document['nonce']).seal(self.ready())
        # A malformed or dangling READY marker is a refusal, not permission to repair.
        (self.root / 'READY.json').unlink()
        (self.root / 'READY.json').symlink_to(self.root / 'absent')
        with self.assertRaisesRegex(provision.ProvisionError, 'after READY'):
            fence.before_effect()

    def test_start_is_once_only_and_preserves_command_failure(self):
        provision.write_new(self.root / 'PREPARED.json', provision.canonical(dict(nonce=self.document['nonce'], host='agt1')))
        commands = mock.Mock()
        host = provision.HostProvision(self.document, 'agt1', self.root, commands)
        host.start()
        self.assertEqual(commands.run.call_args_list, [mock.call(['/usr/bin/systemctl', 'start', row['unit']])
                                                      for row in self.document['runtimes'][:2]])
        with self.assertRaises(FileExistsError):
            host.start()
        commands.run.reset_mock()
        (self.root / 'STARTED.json').unlink()
        commands.run.side_effect = provision.ProvisionError('supervised command failed rc=125')
        with self.assertRaisesRegex(provision.ProvisionError, 'rc=125'):
            host.start()
        self.assertEqual(commands.run.call_count, 1)

    def test_supply_mismatch_fails_before_initial_effect(self):
        binary, supply = self.root / 'binary', self.root / 'supply'
        binary.write_bytes(b'hermetic nonexecutable bytes')
        supply.write_bytes(b'P10 raw fixture')
        self.document['artifact']['binary'] = provision.digest(binary.read_bytes())
        self.document['supply'] = dict(path=str(supply), sha=provision.digest(supply.read_bytes()),
                                       bytes=supply.stat().st_size, schema='P10-supply')
        commands = mock.Mock()
        for bad in ('supply', 'binary'):
            with self.subTest(bad=bad):
                document = copy.deepcopy(self.document)
                if bad == 'supply':
                    document['supply']['sha'] = 'e' * 64
                else:
                    document['artifact']['binary'] = 'e' * 64
                host = provision.HostProvision(document, 'agt1', self.root, commands)
                with mock.patch.object(provision.os, 'chown') as chown, self.assertRaises(provision.ProvisionError):
                    host.prepare(binary, '/observer', '/decoder')
                chown.assert_not_called()
        commands.run.assert_not_called()

    def test_prepare_creates_independent_files_without_starting_or_product_state(self):
        binary, supply = self.root / 'binary', self.root / 'supply'
        binary.write_bytes(b'hermetic nonexecutable bytes')
        supply.write_bytes(b'P10 fixture bytes')
        self.document['artifact']['binary'] = provision.digest(binary.read_bytes())
        self.document['supply'] = dict(path=str(supply), sha=provision.digest(supply.read_bytes()),
                                       bytes=supply.stat().st_size, schema='P10-supply')
        commands = mock.Mock(supervisor=Path('/shared-supervisor'))
        def mapped(path):
            path = Path(path)
            return self.root / str(path).lstrip('/') if str(path).startswith(('/home/sim-', '/opt/s10/', '/etc/systemd/')) else path
        def account(name):
            return SimpleNamespace(pw_dir='/home/' + name, pw_uid=os.getuid(), pw_gid=os.getgid())
        mapped('/etc/systemd/system').mkdir(parents=True)
        original_write = provision.write_new
        # Only filesystem routing and account/chown are synthetic. The real
        # provision algorithm writes/opens its files, enforces exclusivity and
        # renders the actual argv. No system account or systemd is modified.
        with mock.patch.object(provision, 'Path', side_effect=mapped), \
                mock.patch.object(provision.pwd, 'getpwnam', side_effect=account), \
                mock.patch.object(provision.os, 'chown'), \
                mock.patch.object(provision, 'write_new', side_effect=lambda p, raw, mode=0o600: original_write(mapped(p), raw, mode)):
            host = provision.HostProvision(self.document, 'agt1', self.root / 'state', commands)
            host.prepare(binary, '/observer', '/decoder')
            with self.assertRaises(FileExistsError):
                host.prepare(binary, '/observer', '/decoder')
        commands.run.assert_called_once_with(['/usr/bin/systemctl', 'daemon-reload'])
        files = []
        for row in self.document['runtimes'][:2]:
            self.assertEqual(mapped(row['binary']).read_bytes(), binary.read_bytes())
            self.assertEqual(mapped(row['config']).stat().st_mode & 0o777, 0o600)
            self.assertIn(row['binary'], mapped('/etc/systemd/system/' + row['unit']).read_text())
            self.assertEqual((self.root / 'state' / row['runtime']).stat().st_mode & 0o7777, 0o2770)
            files.append(mapped(row['binary']).stat().st_ino)
        self.assertEqual(len(set(files)), 2)
        names = [p.name for p in self.root.rglob('*')]
        self.assertFalse(any(name.endswith('.prev') or name in ('selector', 'journal', 'state.json') for name in names))

    def test_files_reject_hardlinks_symlinks_and_overwrite(self):
        original = self.root / 'original'
        provision.write_new(original, b'immutable', 0o700)
        identity = provision.file_identity(original)
        self.assertEqual(identity['sha'], provision.digest(b'immutable'))
        with self.assertRaises(FileExistsError):
            provision.write_new(original, b'replacement')
        os.link(original, self.root / 'alias')
        with self.assertRaisesRegex(provision.ProvisionError, 'shared or nonregular'):
            provision.file_identity(original)
        (self.root / 'link').symlink_to(original)
        with self.assertRaises(OSError):
            provision.file_identity(self.root / 'link')

    def test_units_and_configs_bind_each_runtime_and_private_custody(self):
        for row in self.document['runtimes']:
            unit = provision.agent_unit(row, '/observer', '/decoder', '/supervise', '/custody/' + row['runtime'])
            self.assertIn('User=' + row['user'] + '\nGroup=' + row['user'], unit)
            self.assertIn('Environment=HOME=' + row['home'], unit)
            self.assertIn('--node ' + row['runtime'] + ' -- ' + row['binary'] + ' agent --session ' + row['sid'], unit)
            self.assertIn('Restart=always', unit)
            self.assertNotIn('restart', unit)
            self.assertNotIn('--pin', unit)
            config = provision.agent_config(row, self.document)
            self.assertIn('session: "' + row['sid'] + '"', config)
            self.assertIn('broker_url: "wss://brk1:443/nats"', config)

    def test_log_roster_includes_all_sids_and_prelogger(self):
        brokers = [dict(host='brk1', unit='tether-broker.service', home='/var/lib/tether',
                        boot_id='fixture-boot', invocation_id='broker-invocation')]
        roster = provision.log_roster(self.ready(), brokers)
        self.assertEqual(sum(r['source_kind'] == 'prelogger-journal' for r in roster['streams']), 5)
        self.assertEqual(len(logs.roster_streams(roster)), 15)
        with self.assertRaises(provision.ProvisionError):
            provision.log_roster(self.ready(), [])

    def test_inspection_joins_real_executable_pid_uid_and_cgroup(self):
        commands = self.commands()
        executable = self.root / 'runtime'
        shutil.copyfile(sys.executable, executable)
        executable.chmod(0o700)
        config = self.root / 'agent.yaml'
        provision.write_new(config, b'fixture config')
        account = pwd.getpwuid(os.getuid())
        row = dict(runtime='a0', host='agt1', unit='fixture.service', sid='s10-small-push', nid='a0',
                   user=account.pw_name, home=account.pw_dir, domain=str(self.root), binary=str(executable),
                   config=str(config), ctl_home=str(self.root / 'ctl'))
        (self.root / 'a0').mkdir()
        # This is an executable inspector fixture, not a tether substitute.
        # The only synthetic observation is systemd's property response.
        (self.root / 'agent').write_text('import os,sys\nprint(os.getpid(), flush=True)\nassert sys.stdin.read(1)=="X"\n')
        with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as endpoint:
            endpoint.bind(str(self.root / 'a0/a0.sock'))
            child = subprocess.Popen([str(commands.supervisor), '--', str(executable), 'agent', '--session', row['sid'],
                                      '--nid', row['nid']], cwd=self.root, stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                pid = int(child.stdout.readline())
                group = Path('/proc/' + str(pid) + '/cgroup').read_text().splitlines()[0].split(':', 2)[-1]
                service = dict(pid=os.getpid(), start_ticks=int(Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]))
                (self.root / 'a0/a0.service.json').write_text(json.dumps(service))
                target = dict(pid=pid, start_ticks=int(Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()[19]),
                              pid_namespace=os.readlink('/proc/' + str(pid) + '/ns/pid'))
                (self.root / 'a0/a0.product.json').write_text(json.dumps(dict(target=target)))
                props = dict(User=row['user'], Group=row['user'], MainPID=str(os.getpid()), ActiveState='active',
                             SubState='running', InvocationID='fixture-invocation', ControlGroup=group, Restart='always')
                fake = mock.Mock()
                fake.run.side_effect = lambda _argv: ''.join(k + '=' + v + '\n' for k, v in props.items())
                receipt = provision.inspect_runtime(row, self.root, fake)
                self.assertEqual(receipt['process']['pid'], pid)
                self.assertEqual(receipt['running_sha'], provision.digest(executable.read_bytes()))
                for key, wrong, criterion in (('User', 'nobody', 'unit not ready'),
                                               ('ControlGroup', '/wrong', 'cgroup differs')):
                    old, props[key] = props[key], wrong
                    with self.assertRaisesRegex(provision.ProvisionError, criterion):
                        provision.inspect_runtime(row, self.root, fake)
                    props[key] = old
                config.chmod(0o644)
                with self.assertRaisesRegex(provision.ProvisionError, 'ownership mismatch'):
                    provision.inspect_runtime(row, self.root, fake)
                config.chmod(0o600)
                replacement = self.root / 'replacement'
                replacement.write_bytes(b'wrong on-disk bytes')
                replacement.replace(executable)
                with self.assertRaisesRegex(provision.ProvisionError, 'executing image differs'):
                    provision.inspect_runtime(row, self.root, fake)
                child.communicate(b'X', timeout=10)
                self.assertEqual(child.returncode, 0)
            finally:
                if child.poll() is None:
                    child.terminate()
                child.communicate(timeout=15)

    def commands(self):
        path = os.environ.get('SIM_UPGRADE_SUPERVISOR', '')
        self.assertTrue(path and Path(path).is_absolute(), 'SIM_UPGRADE_SUPERVISOR must name the built P05 launcher')
        return provision.Commands(path)

    def test_supervision_retains_native_failure_and_cleanup_125(self):
        commands = self.commands()
        self.assertEqual(commands.run([sys.executable, '-I', '-B', '-c', 'print("joined")']).strip(), 'joined')
        for code in (19, 125):
            with self.assertRaisesRegex(provision.ProvisionError, 'rc=' + str(code)):
                commands.run([sys.executable, '-I', '-B', '-c', 'raise SystemExit(' + str(code) + ')'])

    def test_commands_always_enter_shared_supervisor(self):
        commands = self.commands()
        for code in (0, 125):
            child = mock.Mock(returncode=code)
            child.communicate.return_value = (b'fixture', b'')
            manager = mock.MagicMock()
            manager.__enter__.return_value = child
            with mock.patch.object(provision.subprocess, 'Popen', return_value=manager) as spawn:
                if code:
                    with self.assertRaises(provision.CommandError) as caught:
                        commands.run(['/hermetic-target'])
                    self.assertEqual(caught.exception.code, 125)
                else:
                    self.assertEqual(commands.run(['/hermetic-target']), 'fixture')
                self.assertEqual(spawn.call_args.args[0], [str(commands.supervisor), '--', '/hermetic-target'])

    def test_observer_installer_refuses_ready_before_chown_or_restart(self):
        ready = self.root / 'READY.json'
        ready.write_text('{}')
        with mock.patch.object(sys, 'argv', ['observer', '--node', 'brk1', '--nonce', self.document['nonce'],
                                            '--script', '/decoder', '--ready-file', str(ready)]), \
                mock.patch.object(observer.os, 'chown') as chown, \
                mock.patch.object(observer.subprocess, 'check_output') as read, \
                mock.patch.object(observer.subprocess, 'run') as run:
            self.assertEqual(observer.main(), 2)
            chown.assert_not_called()
            read.assert_not_called()
            run.assert_not_called()

    def test_supervised_child_registers_waiter_before_readiness_failure(self):
        children = []
        child = mock.Mock()
        before = len(list(Path('/proc/self/fd').iterdir()))
        with mock.patch.object(observer.subprocess, 'Popen', return_value=child), \
                mock.patch.object(observer.select, 'select', return_value=([], [], [])):
            with self.assertRaisesRegex(ValueError, 'readiness timeout'):
                observer.supervised_child(self.commands().supervisor, self.root, 'partial', ['/fixture'],
                                          subprocess.DEVNULL, children)
        self.assertEqual(children, [child])
        self.assertEqual(len(list(Path('/proc/self/fd').iterdir())), before)

    def test_partial_observer_start_joins_waiter_and_keeps_cleanup_125(self):
        child = mock.Mock()
        child.poll.return_value = None
        child.wait.return_value = 125
        def partial(_supervisor, _root, _label, _argv, _stdin, children):
            children.append(child)
            raise ValueError('synthetic readiness failure')
        before = len(list(Path('/proc/self/fd').iterdir()))
        with mock.patch.object(observer, 'supervised_child', side_effect=partial):
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                observer.launch(self.root, 'a0', LIB / 'flow_observation.py', self.commands().supervisor,
                                [sys.executable, '-I', '-B', '/unused'])
        self.assertEqual(caught.exception.returncode, 125)
        child.terminate.assert_called_once_with()
        child.wait.assert_called_once_with(timeout=15)
        self.assertEqual(len(list(Path('/proc/self/fd').iterdir())), before)

    def test_observer_failure_does_not_cancel_live_product(self):
        keeper, product = mock.Mock(), mock.Mock()
        keeper.poll.return_value = keeper.wait.return_value = 2
        product.poll.side_effect = [None, None, 0, 0]
        product.wait.return_value = 0
        (self.root / 'a0.sock').touch()
        def start(_supervisor, _root, label, _argv, _stdin, children):
            child = keeper if label.endswith('.supervisor') else product
            children.append(child)
            return child
        with mock.patch.object(observer, 'supervised_child', side_effect=start):
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                observer.launch(self.root, 'a0', LIB / 'flow_observation.py', self.commands().supervisor,
                                [sys.executable, '-I', '-B', '/unused'])
        self.assertEqual(caught.exception.returncode, 2)
        product.terminate.assert_not_called()
        keeper.wait.assert_called_once_with(timeout=15)
        product.wait.assert_called_once_with(timeout=15)

    def certificates(self):
        commands = self.commands()
        key, cert = self.root / 'key.pem', self.root / 'cert.pem'
        # Self-signed CA/leaf fixture: real cryptographic checks, no network or
        # production certificate reads. Every openssl process uses P05.
        commands.run(['/usr/bin/openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                      '-subj', '/CN=brk1', '-addext', 'subjectAltName=DNS:brk1',
                      '-keyout', str(key), '-out', str(cert)])
        key.chmod(0o600)
        return cert, key

    def test_wrong_certificate_hostname_key_and_uid_are_rejected(self):
        cert, key = self.certificates()
        sha = provision.digest(cert.read_bytes())
        args = (cert, key, cert, 'brk1', sha, os.getuid(), os.getgroups())
        self.assertTrue(provision.verify_certificate(*args)['verified'])
        with self.assertRaises(ssl.SSLCertVerificationError):
            provision.verify_certificate(cert, key, cert, 'brk2', sha, os.getuid(), os.getgroups())
        with self.assertRaisesRegex(provision.ProvisionError, 'artifact mismatch'):
            provision.verify_certificate(cert, key, cert, 'brk1', 'e' * 64, os.getuid(), os.getgroups())
        with self.assertRaisesRegex(provision.ProvisionError, 'cannot read'):
            provision.verify_certificate(cert, key, cert, 'brk1', sha, 60001, [60001])
        key.chmod(0o604)
        with self.assertRaisesRegex(provision.ProvisionError, 'world accessible'):
            provision.verify_certificate(*args)
        key.chmod(0o600)
        key.write_bytes(b'not a private key')
        with self.assertRaises(ssl.SSLError):
            provision.verify_certificate(*args)

    def test_custody_launch_uses_supervisor_and_passes_original_socket(self):
        commands = self.commands()
        # Five real adapter lifetimes; Go G/FD assertions remain P05/P25's
        # responsibility. This test counts its own FDs and joins every waiter.
        before = len(list(Path('/proc/self/fd').iterdir()))
        for round_number in range(5):
            self.exercise_custody(commands, round_number)
        self.assertEqual(len(list(Path('/proc/self/fd').iterdir())), before)

    def exercise_custody(self, commands, number):
        root = self.root / str(number)
        root.mkdir()
        product = root / 'product.py'
        product.write_text('''import os, pathlib, socket, struct, sys, time
fd = int(os.environ['TETHER_TEST_DRILL_CUSTODY_FD'])
config = os.pread(fd, 32, 0)
assert config[:8] == b'TDCFG1\\0\\0'
channel = socket.socket(fileno=struct.unpack('<Q', config[8:16])[0])
os.close(fd)
channel.send(b'original-producer')
assert channel.recv(128) == b'original-consumer'
channel.close()
root = pathlib.Path(sys.argv[1])
until = time.monotonic() + 10
while not (root / 'a0.supervisor.rc').exists() or not (root / 'a0.supervisor.rc').read_text().strip():
    assert time.monotonic() < until
    time.sleep(.01)
assert (root / 'a0.supervisor.rc').read_text().strip() == '0'
''')
        argv = [str(commands.supervisor), '--', sys.executable, '-I', '-B', str(LIB / 'flow_observer_service.py'),
                'launch', '--root', str(root), '--node', 'a0', '--script', str(LIB / 'flow_observation.py'),
                '--supervisor', str(commands.supervisor), '--', sys.executable, '-I', '-B', str(product), str(root)]
        child = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 10
            while not (root / 'a0.sock').exists():
                self.assertIsNone(child.poll(), 'supervised observer stopped before binding')
                self.assertLess(time.monotonic(), deadline, 'custody bind timeout')
                time.sleep(0.01)
            endpoint, control = observer.open_custody(root, 'a0')
            try:
                endpoint.settimeout(5)
                self.assertEqual(endpoint.recv(128), b'original-producer')
                endpoint.send(b'original-consumer')
            finally:
                endpoint.close()
                control.close()
            output, error = child.communicate(timeout=15)
            self.assertEqual(child.returncode, 0, error.decode())
            self.assertNotIn(b'TDCFG1', output + error)
            for label in ('a0.supervisor', 'a0.product'):
                census = json.loads((root / (label + '.json')).read_bytes())
                for process in census.values():
                    path = Path('/proc') / str(process['pid']) / 'stat'
                    if path.exists():
                        self.assertNotEqual(int(path.read_text().rsplit(')', 1)[1].split()[19]), process['start_ticks'])
                self.assertEqual((root / (label + '.rc')).read_text().strip(), '0')
        finally:
            if child.poll() is None:
                child.terminate()
            child.communicate(timeout=15)

    def test_caddy_ready_guard_blocks_provision_and_start(self):
        marker = self.root / 'READY.json'
        marker.write_text('{}')
        script = '. "$1"; FULLSTACK_READY_FILE=$2; sctl() { :; }; poll_until() { :; }; caddy_prepare brk1; exit $?'
        for function in ('caddy_prepare', 'caddy_start'):
            with self.subTest(function=function), self.assertRaisesRegex(provision.ProvisionError, 'rc=2'):
                self.commands().run(['/bin/sh', '-c', script.replace('caddy_prepare', function),
                                     'fixture', str(LIB / 'caddy.sh'), str(marker)])

    def test_wss_probe_uses_the_required_runtime_ctl_home(self):
        script = '. "$1"; FULLSTACK_PROFILE=1; dexec() { printf "%s\\n" "$@"; }; caddy_probe_wss "$2" brk1 "$3"'
        for runtime, host in (('a0', 'agt1'), ('a3', 'agt2')):
            output = self.commands().run(['/bin/sh', '-c', script, 'fixture', str(LIB / 'caddy.sh'), host, runtime])
            self.assertEqual(output.splitlines(), ['-u', 'sim-' + runtime, host, '--',
                             '/usr/local/bin/tether-test-supervise', '--', '/usr/bin/env',
                             'HOME=/home/sim-' + runtime + '/ctl', '/opt/s10/' + runtime + '/tether',
                             'node', 'ls', '--json', '--nats-url', 'wss://brk1:443/nats'])
        with self.assertRaisesRegex(provision.CommandError, 'rc=2'):
            self.commands().run(['/bin/sh', '-c', script, 'fixture', str(LIB / 'caddy.sh'), 'ctl1', 'a0'])

    def test_caddy_consumes_service_uid_and_validates_certificate_before_start(self):
        script = '''set -eu
INSTANCE=fixture
FULLSTACK_PROFILE=1
FULLSTACK_REMOTE_READY_FILE=/fixture/READY.json
FULLSTACK_CA_FILE=/fixture/ca.pem
FULLSTACK_CERT_SHA=public-fixture-sha
ctr_name() { printf 'fixture-%s' "$1"; }
_secrets_node_dir() { printf '%s' "$FIXTURE_ROOT"; }
secrets_mint_ingress() { :; }
die() { exit 2; }
d() {
    [ "$1" != inspect ] || return 1
    case "$3" in */Caddyfile) cp "$2" "$FIXTURE_ROOT/configured" ;; esac
}
dexec() {
    printf '%s\\n' "$*" >> "$FIXTURE_ROOT/events"
    case "$*" in *'-p User --value') printf '%s\\n' "$SERVICE_USER" ;;
      *'fullstack_provision.py certificate'*) [ "$CERT_FAIL" = 0 ] || return 9 ;;
    esac
}
sctl() { printf 'START %s\\n' "$*" >> "$FIXTURE_ROOT/events"; }
poll_until() { :; }
. "$1"
caddy_prepare brk1 || exit $?
caddy_start brk1
'''
        supplied_config = self.root / 's9-Caddyfile'
        supplied_config.write_text('fixture S9 configuration with /sub/* and /.well-known/tether and /nats\n')
        for user, fail in (('tether', '0'), ('tether', '1'), ('root', '0')):
            events = self.root / 'events'
            events.unlink(missing_ok=True)
            with mock.patch.dict(os.environ, FIXTURE_ROOT=str(self.root), CERT_FAIL=fail, SERVICE_USER=user,
                                 FULLSTACK_CADDY_CONFIG=str(supplied_config),
                                 FULLSTACK_CADDY_CONFIG_SHA=provision.digest(supplied_config.read_bytes())):
                if fail == '0' and user == 'tether':
                    self.commands().run(['/bin/sh', '-c', script, 'fixture', str(LIB / 'caddy.sh')])
                else:
                    with self.assertRaisesRegex(provision.ProvisionError, 'rc=2'):
                        self.commands().run(['/bin/sh', '-c', script, 'fixture', str(LIB / 'caddy.sh')])
            text = events.read_text()
            if user == 'root':
                self.assertNotIn('chown', text)
                self.assertNotIn('START', text)
                continue
            self.assertIn('root:tether /etc/tether/caddy/cert.pem /etc/tether/caddy/key.pem', text)
            self.assertIn('chmod 0640 /etc/tether/caddy/key.pem', text)
            self.assertIn('install -d -o root -g tether -m 0750', text)
            self.assertIn('--hostname brk1 --sha public-fixture-sha --user tether', text)
            if fail == '0':
                self.assertLess(text.index('fullstack_provision.py certificate'), text.index('START'))
                self.assertEqual((self.root / 'configured').read_bytes(), supplied_config.read_bytes())
            else:
                self.assertNotIn('START', text)

    def test_transport_forbids_legacy_setup_in_fullstack_profile(self):
        # The real script sources only synthetic shell functions. No Docker,
        # systemctl, or installed product is reachable in this exercise.
        fake = self.root / 'sim'
        (fake / 'lib').mkdir(parents=True)
        (fake / 'drills/lib').mkdir(parents=True)
        (fake / 'lib/docker.sh').write_text('d() { exit 91; }\ndexec() { exit 92; }\n')
        (fake / 'drills/lib/logs.sh').write_text('sim_log_snapshot() { exit 93; }\n')
        with mock.patch.dict(os.environ, FULLSTACK_PROFILE='1', SIM_UPGRADE_SUPERVISOR=str(self.commands().supervisor)):
            for action in ('policy', 'observe-start', 'observe-stop', 'exec', 'exec-user', 'input', 'ctl'):
                with self.subTest(action=action), self.assertRaisesRegex(provision.ProvisionError, 'rc=2'):
                    self.commands().run(['/bin/sh', str(LIB / 'flow_fixture_transport.sh'), str(fake), 'fixture', '/absent',
                                         action, 'default', self.document['nonce']])
            marker = self.root / 'READY.json'
            marker.write_text('{}')
            with mock.patch.dict(os.environ, FULLSTACK_READY_FILE=str(marker)):
                for action in ('fullstack-exec', 'copy-to'):
                    with self.subTest(action=action), self.assertRaisesRegex(provision.CommandError, 'rc=2'):
                        self.commands().run(['/bin/sh', str(LIB / 'flow_fixture_transport.sh'), str(fake), 'fixture', '/absent',
                                             action, 'agt1', '/fixture/registered', '/fixture/command'])

    # origin: hot-update S10 plan §3.2 / §6 / §10.2 P11-P12 seam.
    # origin: S10 internal review FIX-FULLSTACK2 F3
    def test_peer_owners_keep_source_nonces_and_join_native_status(self):
        flow = provision.flow_module(); peer = flow.peer_module(); fixture = peer.module('flow_fixture')
        # Reuse the loaded real modules rather than recompiling every launch.
        self.enterContext(mock.patch.object(provision, 'flow_module', return_value=flow))
        self.enterContext(mock.patch.object(flow, 'peer_module', return_value=peer))
        script = self.root / 'peer-transport.py'; script.write_text('''import os,pwd,sys
op,node,cgroup,*command=sys.argv[1:]; supervisor=os.environ['SIM_UPGRADE_SUPERVISOR']
assert op in ('fullstack-exec','fullstack-exec-user')
if op=='fullstack-exec-user': user,*command=command; assert user==pwd.getpwuid(os.getuid()).pw_name
os.execv(supervisor,[supervisor,'--lease-fd','0','--lease-timeout','5s','--cgroup',cgroup,'--',*command])
''')
        # Local filesystem/socket operations replace synthetic remote tools,
        # avoiding hundreds of process waits. Keep real supervised original
        # peers, leases and waitpid; the transport argv has its own tests below.
        def local_run(operation, *args, data=None, timeout=30, allowed=(0,)):
            if operation == 'copy-to':
                shutil.copy2(args[0], args[2]); return b''
            if operation == 'fullstack-peer-call':
                return provision.canonical(peer.call(Path(args[1]), json.loads(data)))
            self.assertEqual(operation, 'fullstack-peer-read')
            path = Path(args[1]) / args[2]
            fixture.require(path.exists() or 1 in allowed, 'missing peer file')
            return path.read_bytes() if path.exists() else b''
        cgroup = Path('/proc/self/cgroup').read_text().splitlines()[0].split(':', 2)[2]
        # Distinct roots and owners let three readiness-gated lifetimes overlap.
        before = len(list(Path('/proc/self/fd').iterdir())); ready = provision.threading.Barrier(3)
        def exercise_peer(round_number):
            ready.wait(timeout=10)
            base = self.root / str(round_number); base.mkdir()
            transport = fixture.Transport(SimpleNamespace(here=LIB, instance='hermetic', sim='/unused'))
            transport.prefix, transport.run = [sys.executable, '-I', '-B', str(script)], local_run
            endpoints, bindings = {}, {}
            for row in self.rows:
                name = row['runtime']
                with socket.socket() as listener: listener.bind(('127.0.0.1', 0)); port = listener.getsockname()[1]
                bindings[name] = dict(key=row['key'], route=dict(realm='isolated', nats='nats://unused:4222',
                    wss='wss://unused', client_host='ctl1', target_root=str(base / (name+'t')),
                    client_root=str(base / (name+'c')), host='127.0.0.1', port=port, ssh_user='unused'))
                for role in ('target', 'client'):
                    endpoints[name, role] = dict(cgroup=cgroup, user=pwd.getpwuid(os.getuid()).pw_name, home=str(base),
                        listen_host='127.0.0.1', listen_port=port if role=='target' else 0)
            supply = flow.original_sources(peer, self.document['nonce'], 1, bindings)[:2]
            for source in supply: source['spec']['supply']['drain_seconds'] = 0
            factory = provision.FlowPeers(transport, base / 'evidence', self.document, self.rows, self.online, endpoints)
            bad = copy.deepcopy(supply[0]); bad.pop('nonce')
            with self.assertRaises((KeyError, provision.ProvisionError)): factory.start_peer(bad, 'target')
            cohort = factory.cohort(supply); owners = []
            def start(source, role):
                owner = factory.start_peer(source, role); owners.append(owner)
                if round_number == 5 and role == 'client':
                    owner.call(dict(command='configure', flows=[source['spec']]))
                return owner
            cohort.start_peer = start
            try:
                if round_number == 5:
                    with self.assertRaisesRegex(provision.ProvisionError, 'peer RPC failed'): cohort.start()
                    self.assertEqual(len(owners), 2)
                    self.assertTrue(all(owner.joined and owner.status == 0 for owner in owners))
                    return supply
                cohort.start()
                for owner in owners:
                    environment = dict(row.split(b'=', 1) for row in Path('/proc', str(owner.receipt['identity']['pid']), 'environ').read_bytes().split(b'\0') if row)
                    self.assertEqual(environment[b'SIM_UPGRADE_SUPERVISOR'], (owner.remote+'/supervise').encode())
                    self.assertEqual(environment[b'HOME'], str(base).encode())
                cohort.drain()
                cohort.close()
                self.assertTrue(all(owner.joined and owner.status == 0 for owner in owners))
                self.assertEqual(len(owners), 4)
                for source in supply:
                    remote = Path(source['route']['target_root'])
                    self.assertEqual((remote/'fullstack_flow.py').read_bytes(), (LIB/'fullstack_flow.py').read_bytes())
                    raw = (remote/'tcp.sent').read_bytes()
                    self.assertEqual(len(peer.probe.parse_frames(raw, source['nonce'])), 42)
                    with self.assertRaises(peer.probe.ContinuityError): peer.probe.parse_frames(raw, self.document['nonce'])
            finally:
                for owner in owners:
                    if not owner.joined:
                        owner.cancel(); owner.join()
                    self.assertFalse(owner.pulse.is_alive())
                    self.assertFalse(any(Path('/proc', str(pid)).exists() for pid in (owner.child.target_pid, owner.receipt['identity']['pid'])))
            return supply
        with ThreadPoolExecutor(max_workers=3) as rounds: supply = list(rounds.map(exercise_peer, range(6)))[-1]
        self.assertEqual(len(list(Path('/proc/self/fd').iterdir())), before)
        owner = object.__new__(provision.PeerOwner); owner.joined, owner.lease_error = False, False
        owner.child, owner.log, owner.cancel = mock.Mock(), mock.Mock(), mock.Mock()
        owner.child.wait.return_value = 125
        self.assertEqual(owner.join(), 125)
        owner.child.wait.assert_called_once_with(timeout=10)
        cohort = flow.FlowCohort(self.root/'failed-transport', self.document['nonce'], supply, None)
        cohort.owners = {(name, 'target'): mock.Mock() for name in ('a0', 'a1')}
        for owner in cohort.owners.values():
            owner.call.side_effect = fixture.SetupError('transport_native_rc_125_fullstack-peer-call')
            owner.join.return_value = 125
        with self.assertRaisesRegex(ValueError, 'original flow cleanup failed'): cohort.close()
        for owner in cohort.owners.values(): owner.join.assert_called_once_with()
        self.assertEqual(sum('native_rc_125' in row for row in cohort.cleanup_errors), 2)

    def test_remote_transport_requires_cgroup_and_renewable_lease(self):
        fake = self.root / 'remote'
        (fake / 'lib').mkdir(parents=True)
        (fake / 'drills/lib').mkdir(parents=True)
        (fake / 'lib/docker.sh').write_text('ctr_name() { printf "fixture-%s" "$1"; }; d() { printf "%s\\n" "$@"; }\n')
        (fake / 'drills/lib/logs.sh').write_text('')
        args = ['/bin/sh', str(LIB / 'flow_fixture_transport.sh'), str(fake), 'fixture', '/absent',
                'fullstack-exec', 'agt2', '/fixture/registered', '/fixture/command']
        output = self.commands().run(args).splitlines()
        self.assertEqual(output, ['exec', '-i', 'fixture-agt2', '/usr/local/bin/tether-test-supervise',
                                  '--lease-fd', '0', '--lease-timeout', '5s', '--cgroup', '/fixture/registered',
                                  '--', '/fixture/command'])
        args[-2] = ''
        with self.assertRaisesRegex(provision.CommandError, 'rc=2'):
            self.commands().run(args)

if __name__ == '__main__':
    unittest.main()
