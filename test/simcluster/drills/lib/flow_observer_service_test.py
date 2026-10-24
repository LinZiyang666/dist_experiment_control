#!/usr/bin/env -S python3 -I -B
"""Real Unix custody rejection checks; no product or deployment processes.

The Go assembly test runs this inside the shared supervisor for five rounds.
Rights are kernel-installed FDs, and every sender thread is joined before the
baseline census. This suite certifies cleanup, not product observer coverage.
"""
import array
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import flow_observer_service as service


def descriptors():
    result = {}
    for name in os.listdir('/proc/self/fd'):
        try:
            result[int(name)] = os.readlink('/proc/self/fd/' + name)
        except FileNotFoundError:
            pass
    return result


class ObserverCustodyTest(unittest.TestCase):
    # origin: S10 internal review rv3-assembly F7
    def test_rejected_kernel_rights_are_all_closed(self):
        for kind in ('pipe', 'file', 'datagram', 'unconnected', 'extra', 'truncated', 'bad-magic'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as name:
                root = Path(name)
                before = descriptors()
                server = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
                server.bind(str(root / 'a0.sock')); server.listen(1)
                owned, errors = [], []
                pipe = os.pipe(); owned.extend(pipe)
                if kind == 'file':
                    fd = os.open(root / 'regular', os.O_CREAT | os.O_RDWR, 0o600); owned.append(fd)
                    rights = [fd]
                elif kind in ('datagram', 'unconnected'):
                    endpoint = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM if kind == 'datagram' else socket.SOCK_SEQPACKET)
                    owned.append(endpoint.detach()); rights = [owned[-1]]
                else:
                    rights = [pipe[0]] * (2 if kind == 'extra' else 20 if kind == 'truncated' else 1)
                def send():
                    try:
                        with server.accept()[0] as peer:
                            peer.recv(32)
                            peer.sendmsg([b'invalid' if kind == 'bad-magic' else b'fullstack-custody-v1'],
                                         [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', rights))])
                    except BaseException as error:
                        errors.append(error)
                sender = threading.Thread(target=send)
                sender.start()
                try:
                    with self.assertRaises((OSError, ValueError)):
                        service.open_custody(root, 'a0')
                finally:
                    sender.join(timeout=5)
                    server.close()
                    for fd in owned:
                        os.close(fd)
                self.assertFalse(sender.is_alive(), 'original sender not joined')
                self.assertEqual(errors, [])
                leaked = {fd: target for fd, target in descriptors().items() if fd not in before}
                # A failing mutation is still hermetic: preserve the assertion
                # while closing its surplus FDs before the next exercise.
                for fd in leaked:
                    os.close(fd)
                self.assertEqual(leaked, {}, 'received FD escaped rejection ownership')


if __name__ == '__main__':
    unittest.main()
