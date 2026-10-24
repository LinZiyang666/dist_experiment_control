"""Hermetic adversarial checks for the corresponding fixture responsibility."""
import copy
import ast
import ctypes
import csv
import difflib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import socket
import sys
import tempfile
import tarfile
import time
import unittest

from upgrade_fixture_evidence import (
    C, F, I, S, LIB, SHA, OTHER, ID, CUT, NONCE, OWNER,
    synthetic_observation, contract_rows, synthetic_metrics, synthetic_manifest,
    sealed_pair, drains, evidence, file_reference, synthetic_companion, reseal,
    arm_pair, import_drill, cold_evidence,
)

class UpgradeFixtureSupervision(unittest.TestCase):
    def test_real_socket_reconnect_changes_the_kernel_cookie(self):
        baseline_fds = len(list(Path('/proc/self/fd').iterdir()))
        for round in range(5):
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', 0))
                listener.listen()
                listener.settimeout(3)
                with socket.create_connection(listener.getsockname(), timeout=3) as original:
                    with listener.accept()[0] as peer:
                        original.sendall(b'original-stream')
                        self.assertEqual(peer.recv(32), b'original-stream')
                        cookie = original.getsockopt(socket.SOL_SOCKET, 57, 8).hex()
                        original_fd = original.fileno()
                with socket.create_connection(listener.getsockname(), timeout=3) as replacement:
                    self.assertEqual(original_fd, replacement.fileno())
                    with listener.accept()[0] as peer:
                        replacement.sendall(b'reconnected-stream')
                        self.assertEqual(peer.recv(32), b'reconnected-stream')
                        new_cookie = replacement.getsockopt(socket.SOL_SOCKET, 57, 8).hex()
                        self.assertNotEqual(cookie, new_cookie)
                        row, _, manifest = sealed_pair()
                        resource = row['resources'][0]
                        resource['ofd'].update(identity=cookie, original_identity=cookie)
                        originals = C.seal(row, C.digest(manifest))
                        row['original_set_digest'] = originals['original_set_digest']
                        resource['ofd']['identity'] = new_cookie
                        with self.assertRaises(C.EvidenceError) as caught:
                            C.validate_sample(row, originals, NONCE)
                        self.assertEqual(caught.exception.rc, 3)
        self.assertEqual(baseline_fds, len(list(Path('/proc/self/fd').iterdir())))

    def test_real_unsealed_memfd_is_truncated_without_business_repair(self):
        fd = os.memfd_create('s8-test-transport', flags=0)
        try:
            os.write(fd, b'transport-payload')
            owner = S.identity(os.getpid())
            request = dict(version=1, op='arm', mechanism='transport_truncate', target=dict(owner=owner), restore=None)
            ack = I.kernel(request, None, fd, 3)
            self.assertEqual(os.fstat(fd).st_size, 3)
            request.update(op='release', restore=ack['restore'])
            I.kernel(request, None, fd, None)
            self.assertEqual(os.fstat(fd).st_size, 3, 'injector repaired the transport instead of preserving failure evidence')
        finally:
            os.close(fd)
    def exercise_escaped_descendant(self, round):
        with tempfile.TemporaryDirectory(prefix='s8-escape-') as temporary:
            root = Path(temporary)
            marker = root / 'escaped.json'
            reader, writer = os.pipe()
            child = S.start(root, reader, 20)
            os.close(reader)
            source = """import os,time,json
from pathlib import Path
if os.fork()==0:
 os.setsid()
 if os.fork()==0:
  p=os.getpid(); f=Path('/proc/'+str(p)+'/stat').read_text().rsplit(')',1)[1].split()
  Path(__import__('sys').argv[1]).write_text(json.dumps(dict(pid=p,start_ticks=int(f[19]),boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip())))
  time.sleep(30)
 os._exit(0)
time.sleep(30)
"""
            try:
                S.call(root, dict(op='spawn', id='double-fork', spec=dict(argv=[sys.executable, '-c', source, str(marker)],
                    cwd=str(root), timeout_s=5)))
                deadline = time.monotonic() + 5
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(marker.exists(), 'actual setsid grandchild did not start')
                escaped = json.loads(marker.read_bytes())
                self.assertTrue(S.same_process(escaped))
            finally:
                os.close(writer)
                receipt = S.wait_empty(root)
                child.wait(timeout=12)
            self.assertTrue(receipt['empty'])
            survived = S.same_process(escaped)
            if survived:
                # Cleanup follows the oracle observation and cannot turn the
                # mutation green. The test's own subreaper owns this orphan.
                fd = S.pidfd_open(escaped['pid'])
                try: signal.pidfd_send_signal(fd, signal.SIGKILL)
                finally: os.close(fd)
                os.waitpid(escaped['pid'], 0)
            self.assertFalse(survived, 'M21: escaped setsid process survived lifeline EOF')

    # origin: hot-update S8 plan §9.1 / M21. Direct five-round process exercise;
    # no Go leak anchor is invented for a shell/Python-only package.
    def test_lifeline_eof_reaps_double_fork_setsid_in_five_rounds(self):
        for round in range(5):
            self.exercise_escaped_descendant(round)

    def test_timeout_reaps_a_real_escaped_child(self):
        source = "import os,time\nif os.fork()==0:\n os.setsid()\ntime.sleep(1)"
        with self.assertRaises(S.SupervisionError):
            S.run([sys.executable, '-c', source], .2)

    def test_runner_sigkill_closes_external_supervisor_lifeline(self):
        with tempfile.TemporaryDirectory(prefix='s8-death-') as temporary:
            root = Path(temporary)
            outer = root / 'outer'
            outer.mkdir()
            inner = root / 'inner'
            inner.mkdir()
            source = """import importlib.util,os,time,signal
from pathlib import Path
spec=importlib.util.spec_from_file_location('s',__import__('sys').argv[1]);s=importlib.util.module_from_spec(spec);spec.loader.exec_module(s)
r,w=os.pipe();root=Path(__import__('sys').argv[2]);owner=s.start(root,r,20);os.close(r)
s.call(root,dict(op='spawn',id='escaped',spec=dict(argv=[__import__('sys').executable,'-c',"import os,time; p=os.fork(); p or os.setsid(); p or __import__('pathlib').Path(__import__('sys').argv[1]).write_text('ready'); time.sleep(30)",str(root/'escaped-ready')],cwd=str(root),timeout_s=5)))
deadline=time.monotonic()+5
while not (root/'escaped-ready').exists() and time.monotonic()<deadline:time.sleep(.01)
assert (root/'escaped-ready').exists()
os.kill(os.getpid(),signal.SIGKILL)
"""
            reader, writer = os.pipe()
            child = S.start(outer, reader, 20)
            os.close(reader)
            try:
                S.call(outer, dict(op='spawn', id='runner', spec=dict(argv=[sys.executable, '-c', source,
                    str(LIB / 'upgrade_tx_supervisor.py'), str(inner)], cwd=str(root), timeout_s=5)))
                # Observe inner custody cleanup while the outer supervisor is
                # still live. Outer teardown cannot stand in for lifeline EOF.
                receipt = S.wait_empty(inner)
                self.assertTrue(receipt['empty'])
                self.assertTrue((inner / 'escaped-ready').exists())
                self.assertGreaterEqual(len(receipt['killed']), 2)
                self.assertEqual(S.call(outer, dict(op='census'))['runner']['native_rc'], -signal.SIGKILL)
            finally:
                os.close(writer)
                S.wait_empty(outer)
                child.wait(timeout=12)

    def test_kernel_prlimit_reduces_and_restores_actual_owner(self):
        with tempfile.TemporaryDirectory(prefix='s8-limit-') as temporary:
            root = Path(temporary)
            reader, writer = os.pipe()
            child = S.start(root, reader, 20)
            os.close(reader)
            try:
                row = S.call(root, dict(op='spawn', id='target', spec=dict(argv=[sys.executable, '-c', 'import time;time.sleep(30)'],
                    cwd=str(root), timeout_s=5)))
                request = dict(version=1, op='arm', mechanism='prlimit', target=dict(owner=row), restore=None)
                import resource
                original = resource.prlimit(row['pid'], resource.RLIMIT_NOFILE)
                ack = I.kernel(request, 32, None, None)
                self.assertEqual(resource.prlimit(row['pid'], resource.RLIMIT_NOFILE), (32, original[1]))
                request.update(op='release', restore=ack['restore'])
                I.kernel(request, None, None, None)
                self.assertEqual(resource.prlimit(row['pid'], resource.RLIMIT_NOFILE), original)
                request['target']['owner']['start_ticks'] += 1
                with self.assertRaises(I.collect.EvidenceError):
                    I.kernel(request, None, None, None)
            finally:
                os.close(writer)
                S.wait_empty(root)
                child.wait(timeout=12)
