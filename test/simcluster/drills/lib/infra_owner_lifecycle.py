#!/usr/bin/env -S python3 -I -B
"""Private owner stop/drain protocol and shared-supervisor wait custody.

This module reports lifecycle facts only. A joined helper is never evidence
that product business survived. Product identities/bytes belong to the native
reader and the existing flow/PTY/exec probes.
origin: hot-update S9 plan §9.1 / L22; S9 INSTRUMENTS-2.
"""
import contextlib
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import time

import infra_composer_contract as contract
import hot_update_infra_workload as workload
from infra_owner_plan import need


def private(root):
    root = Path(root)
    info = root.lstat()
    need(root.is_absolute() and root.is_dir() and not root.is_symlink() and
         info.st_uid == os.geteuid() and info.st_mode & 0o077 == 0, 'owner', 'private_evidence_required')
    return root


def birth(pid):
    fields = (Path('/proc') / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()
    need(fields[0] not in ('Z', 'X'), 'owner', 'owner_dead')
    return int(fields[19])


@contextlib.contextmanager
def endpoint(root):
    # The evidence directory can be deeper than sockaddr_un's 108-byte bound.
    fd = os.open(private(root), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        yield '/proc/self/fd/' + str(fd) + '/owner.sock'
    finally:
        os.close(fd)


def receive(peer):
    raw = bytearray()
    while not raw.endswith(b'\n'):
        part = peer.recv(65536)
        need(bool(part) and len(raw) + len(part) <= 1 << 20, 'owner', 'bounded_complete_response_required')
        raw.extend(part)
    return workload.decode(raw)


def request(root, digest, action, timeout=20):
    need(action in ('drain', 'stop', 'sample'), 'owner', 'control_action')
    root = private(root)
    state = contract.read(root / 'lease.json')
    need(state['context_sha256'] == digest and birth(state['pid']) == state['start_ticks'],
         'owner', 'stale_control')
    with endpoint(root) as address, socket.socket(socket.AF_UNIX) as peer:
        peer.settimeout(timeout)
        peer.connect(address)
        pid, uid, _ = struct.unpack('3i', peer.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        need(uid == os.geteuid() and pid == state['pid'] and birth(pid) == state['start_ticks'],
             'owner', 'foreign_control_peer')
        peer.sendall(contract.canonical(dict(action=action, context_sha256=digest)))
        result = receive(peer)
    if action == 'sample':
        need(type(result) is dict and set(result) == {'native_rc', 'action', 'payload'} and
             result['native_rc'] == 0 and result['action'] == action, 'owner', 'sample_failed')
        return result['payload']
    need(result == {'native_rc': 0, 'action': action}, 'owner', 'control_failed')
    return result


class Lease:
    """One foreground owner and one stop acknowledgement, with a fixed expiry."""
    def __init__(self, root, context_sha256, seconds):
        self.root, self.digest = private(root), context_sha256
        need(contract.probe.digest(context_sha256) and type(seconds) is int and 0 < seconds <= 4320,
             'owner', 'lease_contract')
        self.expires = time.monotonic() + seconds + 600

    def serve(self, start, drain, close, intact, observe=None):
        drained, stopped = False, False
        try:
            with endpoint(self.root) as address, socket.socket(socket.AF_UNIX) as server:
                server.bind(address)
                (self.root / 'owner.sock').chmod(0o600)
                server.listen(4)
                server.settimeout(.05)
                start()
                workload.publish(self.root / 'lease.json', dict(pid=os.getpid(), start_ticks=birth(os.getpid()),
                    context_sha256=self.digest, phase='running'))
                while not stopped:
                    need(time.monotonic() < self.expires, 'owner', 'lease_expired')
                    intact()
                    try:
                        peer, _ = server.accept()
                    except socket.timeout:
                        continue
                    with peer:
                        peer.settimeout(2)
                        _, uid, _ = struct.unpack('3i', peer.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                        need(uid == os.geteuid(), 'owner', 'foreign_control_uid')
                        command = receive(peer)
                        need(command in (dict(action='drain', context_sha256=self.digest),
                                         dict(action='stop', context_sha256=self.digest),
                                         dict(action='sample', context_sha256=self.digest)), 'owner', 'control_binding')
                        # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-3.
                        # Read the original foreground owner without draining or
                        # changing producer pacing. Identity checks still run.
                        if command['action'] == 'sample':
                            need(observe is not None, 'owner', 'sample_not_available')
                            payload = observe()
                            intact()
                            peer.sendall(contract.canonical(dict(native_rc=0, action='sample', payload=payload)))
                            continue
                        if not drained:
                            drain()
                            drained = True
                        if command['action'] == 'stop':
                            close()
                            stopped = True
                        peer.sendall(contract.canonical(dict(native_rc=0, action=command['action'])))
            return 0
        finally:
            # Partial start, a broken control channel and SIGTERM all retain
            # the same original waiter. Cleanup failure is never rc0.
            if not stopped:
                close()
            (self.root / 'owner.sock').unlink(missing_ok=True)


class Job:
    """Start through FIX-ORPHAN and preserve the original native wait status."""
    def __init__(self, root, argv, supervisor, expected):
        from flow_fixture_peer import start_supervised
        self.root, self.child, self.status, self.expected = private(root), None, None, expected
        need(os.environ.get('SIM_UPGRADE_SUPERVISOR') == supervisor, 'owner', 'supervisor_binding')
        with (self.root / 'stdout').open('xb') as out, (self.root / 'stderr').open('xb') as err:
            self.child = start_supervised(argv, stdout=out, stderr=err)

    def intact(self):
        need(self.status is not None or self.child.poll() is None, 'owner', 'original_exited_early')

    def join(self, timeout=20):
        if self.status is None:
            self.status = self.child.wait(timeout=timeout)
            workload.publish(self.root / 'wait.json', dict(native_rc=self.status,
                expected_rc=self.expected, joined=True))
        need(self.status == self.expected, 'owner', 'original_native_status')

    def close(self):
        if self.status is not None:
            return
        natural = self.child.poll() is not None
        if not natural:
            self.child.terminate()
        try:
            self.status = self.child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.child.kill()
            self.status = self.child.wait(timeout=5)
        workload.publish(self.root / 'wait.json', dict(native_rc=self.status,
            expected_rc=self.expected, joined=True, emergency=not natural))
        need(natural and self.status == self.expected, 'owner', 'emergency_cleanup')


def interrupted(_number, _frame):
    raise ValueError('NOT_RUN owner: interrupted')


def signals():
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
