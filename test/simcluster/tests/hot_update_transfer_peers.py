#!/usr/bin/env python3
"""Hermetic reader controls; no product/deploy receipt is claimed here.

Real files, sealed memfds, seqpacket custody and loopback NATS protocol IO are
used. The custody producer and protocol server are explicitly controlled test
peers. Oracle controls spawn no process; driver startup controls launch the
actual keeper under the shared Go supervisor. No installed tether/home is used.
"""
import array
import base64
import contextlib
import copy
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

LIB = Path(__file__).resolve().parents[1] / 'drills/lib'
sys.path.insert(0, str(LIB))
import transfer_fixture as fixture
import transfer_keeper as keeper
import transfer_observation as observation
import transfer_oracle as oracle
import transfer_fixture_manifest as fixture_manifest


def canonical(kind):
    return json.dumps(dict(transfer_id='original', kind=kind), separators=(',', ':')).encode()


def event(kind, seq, **extra):
    identity = keeper.process_identity(os.getpid())
    value = dict(schema=1, kind=kind, pid=identity['pid'], process_start=identity['process_start'],
                 boot_id=identity['boot_id'], generation=1, cut_id='', transfer_id='original',
                 entry_nonce='nonce', object_version='NUID', leg='leg2', phase='receiving',
                 event_seq=seq, boottime_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME))
    value.update(extra)
    return value


def envelope(identity, incarnation, sequence, events, poison=0):
    raw = bytearray(b'TDRILL1\0' + identity + incarnation + struct.pack('<QQI', sequence, poison, len(events)))
    for row in events:
        kind, domain, payload = b'transfer-v1', b'transfer', json.dumps(row).encode()
        raw += bytes((len(kind),)) + kind + bytes((len(domain),)) + domain
        raw += bytes(92) + struct.pack('<17Q', row['boottime_ns'], *([0] * 16))
        raw += struct.pack('<I', len(payload)) + payload
    return bytes(raw)


class ProtocolServer:
    """Test peer speaking the actual read-only NATS API wire, not a scan stub."""
    def __init__(self, payload):
        self.lock, self.stopping = threading.Lock(), threading.Event()
        self.config = dict(storage='file', duplicate_window=120_000_000_000)
        self.rows = {'history': [('history.original', canonical('start')),
                                 ('history.original', canonical('complete'))],
                     'OBJ_bucket': [('$O.bucket.C.NUID', payload), ('$O.bucket.M.b2JqZWN0',
                         json.dumps(dict(nuid='NUID', size=len(payload), chunks=1,
                             digest='SHA-256=' + base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).decode())).encode())]}
        self.server = socket.socket()
        self.server.bind(('127.0.0.1', 0))
        self.server.listen(8)
        self.server.settimeout(.1)
        self.url = 'nats://127.0.0.1:' + str(self.server.getsockname()[1])
        self.errors = []
        self.thread = threading.Thread(target=self.run, name='test-nats-wire')
        self.thread.start()

    def append(self, stream, subject, data):
        with self.lock:
            self.rows[stream].append((subject, data))

    def run(self):
        while not self.stopping.is_set():
            try:
                connection, _ = self.server.accept()
            except socket.timeout:
                continue
            try:
                with connection:
                    connection.settimeout(5)
                    self.serve(connection)
            except (OSError, ValueError, AssertionError) as error:
                if not self.stopping.is_set():
                    self.errors.append(type(error).__name__)

    def serve(self, connection):
        connection.sendall(b'INFO {}\r\n')
        subscriptions = {}
        with connection.makefile('rb') as source:
            while not self.stopping.is_set():
                line = source.readline()
                if not line:
                    return
                words = line.strip().split()
                if words[0] == b'PING':
                    connection.sendall(b'PONG\r\n')
                elif words[0] == b'PUB':
                    subject, reply, length = words[1].decode(), words[2], int(words[3])
                    request = json.loads(source.read(length))
                    assert source.read(2) == b'\r\n'
                    with self.lock:
                        stream = subject.rsplit('.', 1)[1]
                        assert stream in self.rows
                        if subject.startswith('$JS.API.STREAM.INFO.'):
                            value = dict(created='fixed-created', state=dict(last_seq=len(self.rows[stream]),
                                         first_seq=1, messages=len(self.rows[stream])))
                            if self.config is not None:
                                value['config'] = dict(self.config, name=stream)
                        else:
                            assert subject.startswith('$JS.API.STREAM.MSG.GET.')
                            sequence = request['seq']
                            if sequence > len(self.rows[stream]):
                                value = dict(error=dict(err_code=10037))
                            else:
                                selected, raw = self.rows[stream][sequence - 1]
                                value = dict(message=dict(seq=sequence, subject=selected,
                                             data=base64.b64encode(raw).decode()))
                    data = json.dumps(value).encode()
                    connection.sendall(b'MSG ' + reply + b' ' + subscriptions[reply] + b' ' + str(len(data)).encode()
                                       + b'\r\n' + data + b'\r\n')
                elif words[0] == b'SUB':
                    subscriptions[words[1]] = words[2]
                else:
                    assert words[0] in (b'CONNECT', b'UNSUB')

    def close(self):
        self.stopping.set()
        self.thread.join(6)
        self.server.close()
        assert not self.thread.is_alive(), 'test wire peer did not join'


class Producer:
    def __init__(self, channel, identity):
        self.channel, self.identity, self.incarnation = channel, identity, os.urandom(16)
        self.sequence, self.ignore = 0, False
        self.lock = threading.RLock()
        self.on_drain, self.drains = None, 0
        self.stop = threading.Event()
        self.channel.settimeout(.1)
        self.thread = threading.Thread(target=self.run, name='test-custody-producer')

    def emit(self, rows):
        with self.lock:
            self.emit_locked(rows)

    def emit_locked(self, rows):
        self.sequence += 1
        fd = observation.sealed(envelope(self.identity, self.incarnation, self.sequence, rows))
        try:
            self.channel.sendmsg([b'TDC1'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [fd]))])
        finally:
            os.close(fd)

    def run(self):
        while not self.stop.is_set():
            try:
                raw = self.channel.recv(64)
                if not raw:
                    return
                assert len(raw) == 20 and raw[:4] == b'TDP2'
                self.drains += 1
                if self.on_drain:
                    self.on_drain(self.drains)
                if not self.ignore:
                    with self.lock:
                        self.emit_locked([])
                        self.channel.send(b'TDA2' + raw[4:] + struct.pack('<QQ', self.sequence, 0))
            except socket.timeout:
                pass
            except OSError:
                if not self.stop.is_set():
                    raise

    def close(self):
        self.stop.set()
        if self.thread.is_alive():
            self.thread.join(1)
        self.channel.close()
        assert not self.thread.is_alive(), 'test custody peer did not join'


@contextlib.contextmanager
def live_fixture():
    with tempfile.TemporaryDirectory(prefix='transfer-fixture-') as directory:
        root, payload = Path(directory), b'new complete target' * 64
        target = root / 'target'
        old = b'old complete target' * 64
        target.write_bytes(old)
        wire = ProtocolServer(payload)
        identity = keeper.process_identity(os.getpid())
        manifest = dict(schema=1, mode='hermetic', quiet_seconds=2, cuts=[],
                        processes=[dict(label='ctl1', artifact_shas=[identity['running_sha']])],
                        transfers=[dict(transfer_id='original', entry_nonce='nonce', object_version='NUID', object_name='object',
                            bucket='bucket', session_id='session', cohort='hermetic', target=str(target),
                            old=[len(old), hashlib.sha256(old).hexdigest()], size=len(payload),
                            sha256=hashlib.sha256(payload).hexdigest(), object_stream='OBJ_bucket',
                            history_stream='history', history_subject='history.original', required_resources=[],
                            start_b64=base64.b64encode(canonical('start')).decode(),
                            terminal_b64=base64.b64encode(canonical('complete')).decode())], nats=dict(url=wire.url))
        manifest_path = root / 'manifest.json'
        manifest_path.write_text(json.dumps(manifest))
        instance = fixture.Fixture(fixture.read_manifest(manifest_path), root / 'evidence')
        channel, config = instance.keeper.lease('ctl1')
        os.close(config)
        producer = Producer(channel, instance.keeper.identities['ctl1'])
        producer.emit([
            event('append', 1, index=0, offset=0, length=len(payload), sha256=hashlib.sha256(payload).hexdigest(),
                  actual_ack_seq=1),
            event('primitive_exit', 2, primitive='rename', outcome='landed'),
            event('directory_sync', 3, parent_synced=True),
            event('retirement', 4, admission_closed=True, retirement_digest='d' * 64, stop_nonce='joined'),
        ])
        # Atomic rename occurs before collecting the controlled product effect.
        # This is a test fixture, not a substitute for a production primitive.
        temp = root / 'new-target'
        temp.write_bytes(payload)
        os.replace(temp, target)
        producer.thread.start()
        instance.start()
        try:
            deadline = time.monotonic() + 5
            complete = False
            while time.monotonic() < deadline:
                with instance.keeper.lock:
                    complete = len(instance.keeper.records.events) == 4 and 'ctl1' in instance.keeper.processes
                if complete:
                    break
                instance.keeper.stop_event.wait(.01)
            assert complete, instance.keeper.setup_failures
            yield instance, producer, wire, target, payload
        finally:
            instance.close()
            producer.close()
            wire.close()
            assert not wire.errors, wire.errors



# origin: S7 internal review rv-drill F1. Explicit protocol peer executed by
# LaunchOwner's real shared-supervisor/bootstrap path, never a product receipt.
def launched_custody_peer():
    assert os.environ['TETHER_TEST_DRILL_OBSERVER']=='v1'
    config = os.pread(int(os.environ['TETHER_TEST_DRILL_CUSTODY_FD']), 32, 0)
    peer = Producer(socket.socket(fileno=3), hashlib.sha256(config).digest())
    peer.emit([event('append', 1, index=0, offset=0, length=len(b'original physical bytes'), actual_ack_seq=1,
                     sha256=hashlib.sha256(b'original physical bytes').hexdigest()),
               event('primitive_exit', 2, primitive='rename', outcome='landed'),
               event('directory_sync', 3, parent_synced=True),
               event('retirement', 4, admission_closed=True, retirement_digest='d'*64, stop_nonce='joined')])
    try:
        peer.run()
    finally:
        peer.close()

