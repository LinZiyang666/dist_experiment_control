#!/usr/bin/env python3
"""Hermetic custody adversaries: actual memfd/SCM_RIGHTS, no product or deploy.

The protocol peer below exercises collection, never supplies product acceptance
receipts. P25 owns the direct Go leak anchors and real collector/product rounds.
"""
import array
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fullstack_collect as collect
import fullstack_logs_test as log_fixture

IDENTITY = hashlib.sha256(b'hermetic-startup-custody').digest()
INCARNATION = bytes(range(1, 17))
SOURCE = 'roster/agent/sid-four'


def event(kind='domain-live', domain='exec', payload=b'private-suffix', nonce=bytes(12)):
    return (bytes([len(kind)]) + kind.encode() + bytes([len(domain)]) + domain.encode() +
            bytes(48) + hashlib.sha256(payload).digest() + nonce + struct.pack('<17Q', 10, *([0] * 16)) +
            struct.pack('<I', len(payload)) + payload)


def batch(sequence=1, events=(), incarnation=INCARNATION, identity=IDENTITY, poison=0):
    return (b'TDRILL1\0' + identity + incarnation + struct.pack('<QQI', sequence, poison, len(events)) +
            b''.join(events))


def send_batch(channel, raw, sealed=True):
    if sealed:
        fd = collect.flow.sealed(raw)
    else:
        fd = os.memfd_create('unsealed-control', os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        os.write(fd, raw)
    try:
        channel.sendmsg([b'TDC1'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [fd]))])
    finally:
        os.close(fd)


class Peer:
    """Only a local wire fixture; original drain challenges come from Collector."""
    def __init__(self, mode='normal'):
        self.channel, consumer = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.consumer = consumer
        self.mode, self.sequence, self.previous, self.error = mode, 0, None, None
        self.thread = threading.Thread(target=self.run, name='hermetic-custody-peer')
        self.thread.start()

    def run(self):
        try:
            if self.mode == 'ack-without-batch':
                self.sequence = 1
                send_batch(self.channel, batch(1))
            while True:
                raw = self.channel.recv(64)
                if not raw:
                    return
                if len(raw) != 20 or raw[:4] != b'TDP2':
                    raise AssertionError('collector sent an unsupported drain')
                if self.mode == 'missing-ack':
                    continue
                if self.mode == 'runtime-pending':
                    send_batch(self.channel, batch(events=[event('runtime-sample', 'agent')]))
                    continue
                if self.mode != 'ack-without-batch':
                    self.sequence += 1
                    send_batch(self.channel, batch(self.sequence, [event()]))
                ack = b'TDA2' + raw[4:] + struct.pack('<QQ', self.sequence, 0)
                if self.mode == 'replay' and self.previous is not None:
                    ack = self.previous
                if self.mode == 'wrong-nonce':
                    ack = b'TDA2' + bytes(16) + ack[20:]
                self.channel.sendall(ack)
                self.previous = ack
        except (BrokenPipeError, ConnectionResetError):
            pass
        except BaseException as error:
            self.error = error

    def join(self):
        self.thread.join(2)
        self.channel.close()
        if self.thread.is_alive():
            raise AssertionError('hermetic protocol peer did not join')
        if self.error:
            raise self.error


# origin: hot-update S10 plan §5.3 / §10.2 P16
class FullstackCustodyCollectorTest(unittest.TestCase):
    def make_collector(self, mode='normal', **limits):
        peer = Peer(mode)
        collector = collect.Collector({SOURCE: (peer.consumer, IDENTITY)}, **limits)
        def cleanup():
            collector.close()
            try:
                collector.join(2)
            except (collect.SetupError, collect.PendingObservation):
                pass
            peer.join()
        self.addCleanup(cleanup)
        return collector, peer

    def wait_poison(self, collector):
        with collector.condition:
            self.assertTrue(collector.condition.wait_for(lambda: collector.failure or
                            collector.pending_dependency, 2), 'intake failed to report broken evidence')

    def test_five_real_lifecycles_close_and_join_every_worker_and_fd(self):
        before_fds = set(os.listdir('/proc/self/fd'))
        before_threads = {t.ident for t in threading.enumerate()}
        for round in range(5):
            peer = Peer()
            collector = collect.Collector({SOURCE: (peer.consumer, IDENTITY)})
            try:
                receipt = collector.seal('round-' + str(round))
                self.assertEqual(receipt['first'][0]['sequence'], 1)
                self.assertEqual(receipt['second'][0]['sequence'], 2)
                self.assertGreater(receipt['second'][0]['finished_ns'], receipt['first'][0]['finished_ns'])
                self.assertFalse(collector.lifecycle()['complete'], 'Flush cannot stand in for Join')
                fd = collector.borrow_frame(0)
                try:
                    self.assertFalse(os.get_inheritable(fd))
                    self.assertEqual(os.pread(fd, os.fstat(fd).st_size, 0), batch(1, [event()]))
                    self.assertEqual(fcntl.fcntl(fd, fcntl.F_GET_SEALS) & collect.flow.SEALS, collect.flow.SEALS)
                finally:
                    collector.release_frame(fd)
                collector.close()
                self.assertFalse(collector.lifecycle()['complete'], 'Close must not claim joined workers')
                collector.join(2)
                self.assertTrue(collector.lifecycle()['complete'])
                self.assertEqual(collector.lifecycle()['rc'], 0)
            finally:
                collector.close()
                collector.join(2)
                peer.join()
        self.assertEqual({t.ident for t in threading.enumerate()}, before_threads)
        self.assertEqual(set(os.listdir('/proc/self/fd')), before_fds)

    def test_fresh_ack_replay_is_sticky_setup_red(self):
        collector, _ = self.make_collector('replay')
        collector.drain(SOURCE, 'first')
        with self.assertRaisesRegex(collect.SetupError, 'custody_fresh_ack_replayed'):
            collector.drain(SOURCE, 'second')
        self.assertEqual(collector.lifecycle()['rc'], 2)
        with self.assertRaisesRegex(collect.SetupError, 'custody_fresh_ack_replayed'):
            collector.drain(SOURCE, 'third')

    def test_wrong_nonce_missing_ack_and_ack_without_new_batch_are_red(self):
        for mode, criterion in [('wrong-nonce', 'custody_fresh_ack_replayed'),
                                ('missing-ack', 'custody_barrier_timeout'),
                                ('ack-without-batch', 'custody_fresh_ack_missing_batch')]:
            with self.subTest(mode=mode):
                collector, _ = self.make_collector(mode)
                if mode == 'ack-without-batch':
                    with collector.condition:
                        self.assertTrue(collector.condition.wait_for(lambda: collector.frames, 2))
                with self.assertRaisesRegex(collect.SetupError, criterion):
                    collector.drain(SOURCE, 'challenge', .1)
                self.assertEqual(collector.lifecycle()['rc'], 2)

    def test_byte_ceiling_preserves_old_sealed_batch(self):
        first_raw = batch(1, [event(payload=b'x' * 512)])
        collector, peer = self.make_collector(byte_limit=len(first_raw) + 76)
        send_batch(peer.channel, first_raw)
        with collector.condition:
            self.assertTrue(collector.condition.wait_for(lambda: len(collector.frames) == 1, 2))
        send_batch(peer.channel, batch(2, [event(payload=b'y' * 512)]))
        self.wait_poison(collector)
        self.assertEqual(collector.failure, 'custody_capacity')
        self.assertEqual(len(collector.frames), 1, 'overflow must not discard historical batches')
        frame = collector.frames[0]
        self.assertEqual(os.pread(frame['fd'], frame['bytes'], frame['offset']), first_raw)
        self.assertEqual(collector.bytes, len(first_raw))
        self.assertEqual(collector.lifecycle()['rc'], 2)

    def test_descriptor_ceiling_preserves_previous_batch(self):
        collector, peer = self.make_collector(fd_limit=4)
        send_batch(peer.channel, batch(1))
        with collector.condition:
            self.assertTrue(collector.condition.wait_for(lambda: len(collector.frames) == 1, 2))
        send_batch(peer.channel, batch(2))
        self.wait_poison(collector)
        self.assertEqual(collector.failure, 'custody_descriptor_capacity')
        self.assertEqual(len(collector.frames), 1)

    def test_compaction_preserves_every_original_frame_and_cursor(self):
        collector, peer = self.make_collector()
        for seq in range(1, 70):
            send_batch(peer.channel, batch(seq, [event(payload=bytes([seq]))]))
        with collector.condition:
            self.assertTrue(collector.condition.wait_for(lambda: len(collector.frames) == 69 or
                            collector.failure, 2))
        self.assertIsNone(collector.failure)
        self.assertLess(collector._fd_count(), 10)
        for cursor in (0, 31, 63, 64, 68):
            fd = collector.borrow_frame(cursor)
            try:
                self.assertEqual(os.pread(fd, os.fstat(fd).st_size, 0),
                                 batch(cursor + 1, [event(payload=bytes([cursor + 1]))]))
            finally:
                collector.release_frame(fd)

    def test_gap_poison_unknown_kind_and_truncation_close_received_fds(self):
        defects = [batch(2), batch(poison=2), batch(events=[event(kind='invented-kind')]),
                   batch(events=[event(kind='transfer-v1', domain='agent')]), batch(incarnation=bytes(16)),
                   batch(events=[event(domain='invented-domain')]), batch()[:-1],
                   batch(events=[event()]) + b'extra', batch(identity=bytes(32))]
        for raw in defects:
            with self.subTest(sha=collect.digest(raw)):
                collector, peer = self.make_collector()
                send_batch(peer.channel, raw)
                self.wait_poison(collector)
                self.assertEqual(collector.lifecycle()['rc'], 2)
                self.assertEqual(collector.frames, [])
                self.assertEqual(collector.bytes, 0)

    def test_unsealed_and_extra_rights_are_rejected(self):
        collector, peer = self.make_collector()
        send_batch(peer.channel, batch(), sealed=False)
        self.wait_poison(collector)
        self.assertEqual(collector.failure, 'custody_unsealed')
        collector2, peer2 = self.make_collector()
        fds = [collect.flow.sealed(batch()) for _ in range(2)]
        try:
            peer2.channel.sendmsg([b'TDC1'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', fds))])
        finally:
            for fd in fds:
                os.close(fd)
        self.wait_poison(collector2)
        self.assertEqual(collector2.failure, 'custody_reply_invalid')

    def test_incarnation_replay_and_identity_change_are_rejected(self):
        collector, peer = self.make_collector()
        send_batch(peer.channel, batch(1))
        send_batch(peer.channel, batch(1, incarnation=b'next-incarnation'))
        send_batch(peer.channel, batch(2))
        self.wait_poison(collector)
        self.assertEqual(collector.failure, 'custody_replayed_incarnation')
        collector2, peer2 = self.make_collector()
        send_batch(peer2.channel, batch(identity=b'x' * 32))
        self.wait_poison(collector2)
        self.assertEqual(collector2.failure, 'custody_identity_changed')

    def test_channel_eof_and_unsolicited_old_ack_are_setup_red(self):
        collector, peer = self.make_collector()
        peer.channel.shutdown(socket.SHUT_WR)
        self.wait_poison(collector)
        self.assertEqual(collector.failure, 'custody_channel_incomplete')
        collector2, peer2 = self.make_collector()
        peer2.channel.sendall(b'TDA2' + bytes(16) + struct.pack('<QQ', 1, 0))
        self.wait_poison(collector2)
        self.assertEqual(collector2.failure, 'custody_fresh_ack_replayed')

    def test_public_receipt_contains_no_private_event_data(self):
        collector, _ = self.make_collector()
        receipt = collector.seal('opaque-public-query')
        raw = json.dumps(receipt)
        self.assertNotIn('private-suffix', raw)
        self.assertNotIn(b'private-suffix'.hex(), raw)
        self.assertNotIn(base64.b64encode(b'private-suffix').decode(), raw)
        self.assertNotIn('domain-live', raw)
        self.assertNotIn(INCARNATION.hex(), raw)
        self.assertNotIn(IDENTITY.hex(), raw)
        self.assertNotIn(SOURCE, raw)
        self.assertGreater(len(receipt['second'][0]['frames']), 0)

    def test_transfer_events_validate_even_when_other_domains_share_batch(self):
        collector, peer = self.make_collector()
        row = dict(schema=1, kind='synced', process_start=1, boot_id='boot', pid=123, generation=1,
                   cut_id='cut', transfer_id='original', entry_nonce='entry', object_version='object',
                   leg='leg2', phase='live', event_seq=1, boottime_ns=10)
        send_batch(peer.channel, batch(1, [event(), event('transfer-v1', 'transfer', json.dumps(row).encode())]))
        row['event_seq'] = 3
        send_batch(peer.channel, batch(2, [event('transfer-v1', 'transfer', json.dumps(row).encode())]))
        self.wait_poison(collector)
        self.assertEqual(collector.failure, 'observation_sequence_gap')
        self.assertEqual(len(collector.frames), 1)

    def test_runtime_dependency_is_pending_and_cannot_become_zero_success(self):
        collector, _ = self.make_collector()
        with self.assertRaisesRegex(collect.PendingObservation, 'pending P06/P07'):
            collector.runtime_sample(SOURCE, 'missing-producer')
        if 'runtime-sample' not in collect.flow.KINDS:
            fd = collect.flow.sealed(batch(events=[event('runtime-sample', 'agent')]))
            try:
                with self.assertRaisesRegex(collect.PendingObservation, 'pending P06'):
                    collect.decode_batch(fd)
            finally:
                os.close(fd)

    def test_missing_runtime_decoder_is_pending_in_actual_intake(self):
        collector, peer = self.make_collector()
        with patch.object(collect.flow, 'KINDS', collect.flow.KINDS - {'runtime-sample'}):
            send_batch(peer.channel, batch(events=[event('runtime-sample', 'agent')]))
            self.wait_poison(collector)
            self.assertIsNone(collector.failure)
            self.assertEqual(collector.lifecycle()['rc'], 4)
            with self.assertRaisesRegex(collect.PendingObservation, 'pending P06'):
                collector.drain(SOURCE, 'never-green')

    def test_pending_dependency_during_barrier_keeps_native_incomplete(self):
        collector, _ = self.make_collector('runtime-pending')
        with patch.object(collect.flow, 'KINDS', collect.flow.KINDS - {'runtime-sample'}):
            with self.assertRaisesRegex(collect.PendingObservation, 'pending P06'):
                collector.drain(SOURCE, 'pending-in-flight')
        self.assertIsNone(collector.failure)
        self.assertEqual(collector.lifecycle()['rc'], 4)

    def test_ofd_witness_releases_only_received_duplicates(self):
        collector, peer = self.make_collector()
        with tempfile.TemporaryFile() as original:
            original.write(b'original-OFD-data')
            original.flush()
            metadata = dict(schema=1, process_start=1, boot_id='hermetic-boot', pid=os.getpid(),
                            resource=dict(resource_id='opaque-resource', kind='xfer-source-v1', name='source'),
                            witness_digest='a' * 64, owned_fds=1, peak_fds=2)
            fd = collect.flow.sealed(json.dumps(metadata).encode())
            try:
                peer.channel.sendmsg([b'TDO1'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                                array.array('i', [fd, original.fileno()]))])
            finally:
                os.close(fd)
            with collector.condition:
                self.assertTrue(collector.condition.wait_for(lambda: collector.witnesses or collector.failure, 2))
            self.assertIsNone(collector.failure)
            witness = collector.witnesses[0]
            self.assertTrue(all(not os.get_inheritable(fd) for fd in witness))
            self.assertEqual(os.pread(witness[1], 17, 0), b'original-OFD-data')
            collector.seal('ofd-observation')
            collector.close()
            collector.join(2)
            for fd in witness:
                with self.assertRaises(OSError):
                    os.fstat(fd)
            self.assertEqual(os.pread(original.fileno(), 17, 0), b'original-OFD-data')

    def test_each_roster_source_has_two_independent_fresh_drains(self):
        peers = [Peer() for _ in range(2)]
        sources = {SOURCE + str(i): (p.consumer, IDENTITY) for i, p in enumerate(peers)}
        collector = collect.Collector(sources)
        try:
            seal = collector.seal('whole-roster')
            self.assertEqual(len(seal['first']), 2)
            self.assertEqual(len(seal['second']), 2)
            self.assertEqual(len({r['source'] for r in seal['second']}), 2)
            self.assertEqual([r['sequence'] for r in seal['second']], [2, 2])
            self.assertEqual([len(r['frames']) for r in seal['second']], [1, 1])
            self.assertEqual(len(collector.frames), 4)
        finally:
            collector.close()
            try:
                collector.join(2)
            finally:
                for peer in peers:
                    peer.join()

    def test_ofd_invalid_schema_resource_and_descriptor_are_rejected(self):
        with tempfile.TemporaryFile() as original:
            base = dict(schema=1, process_start=1, boot_id='hermetic-boot', pid=os.getpid(),
                        resource=dict(resource_id='opaque', kind='xfer-source-v1', name='source'),
                        witness_digest='a' * 64, owned_fds=1, peak_fds=2)
            for defect in ('schema', 'resource', 'digest', 'unknown-field', 'descriptor'):
                with self.subTest(defect=defect):
                    collector, peer = self.make_collector()
                    row = json.loads(json.dumps(base))
                    if defect == 'schema':
                        row['schema'] = True
                    elif defect == 'resource':
                        row['resource']['kind'] = 'invented-kind'
                    elif defect == 'digest':
                        row['witness_digest'] = 'z' * 64
                    elif defect == 'unknown-field':
                        row['unexpected'] = 'never-public'
                    metadata = collect.flow.sealed(json.dumps(row).encode())
                    witness = peer.channel.fileno() if defect == 'descriptor' else original.fileno()
                    try:
                        peer.channel.sendmsg([b'TDO1'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                                        array.array('i', [metadata, witness]))])
                    finally:
                        os.close(metadata)
                    self.wait_poison(collector)
                    self.assertEqual(collector.witnesses, [])
                    self.assertEqual(collector.bytes, 0)
                    self.assertEqual(collector.lifecycle()['rc'], 2)

    def test_close_without_join_and_join_without_close_cannot_sign(self):
        collector, _ = self.make_collector()
        collector.seal('before-close')
        with self.assertRaisesRegex(collect.SetupError, 'custody_close_required'):
            collector.join(.1)
        collector.close()
        self.assertEqual(collector.lifecycle()['rc'], 4)
        self.assertFalse(collector.lifecycle()['joined'])
        collector.join(2)
        self.assertTrue(collector.lifecycle()['joined'])

    def test_borrowed_copies_count_toward_cap_and_must_be_released(self):
        collector, _ = self.make_collector(fd_limit=5)
        collector.seal('with-borrows')
        first, second = collector.borrow_frame(0), collector.borrow_frame(1)
        with self.assertRaisesRegex(collect.SetupError, 'custody_descriptor_capacity'):
            collector.borrow_frame(0)
        collector.release_frame(first)
        collector.release_frame(second)
        third = collector.borrow_frame(0)
        collector.close()
        with self.assertRaisesRegex(collect.SetupError, 'custody_borrow_unreleased'):
            collector.join(2)
        with self.assertRaises(OSError):
            os.fstat(third)

    def test_empty_close_scope_is_incomplete_without_double_drain(self):
        collector, _ = self.make_collector()
        collector.close()
        collector.join(2)
        self.assertEqual(collector.lifecycle()['rc'], 4)

    # origin: S10 internal review FIX-FULLSTACK2 F1
    def test_five_log_scopes_poll_drain_close_and_join_actual_readers(self):
        before = set(os.listdir('/proc/self/fd'))
        # Ordinary temporary files cannot certify retention. Keep that refusal
        # sticky through Join before supplying a reader-only fixture certificate.
        with tempfile.TemporaryDirectory() as root:
            logs = log_fixture.Fixture(root)
            logs.capture.start(logs.roster)
            collector, peer = self.make_collector(logs=logs.capture)
            with self.assertRaisesRegex(collect.SetupError, 'custody_log_drain_failure'):
                collector.seal('uncertified-logs')
            collector.close()
            with self.assertRaisesRegex(collect.SetupError, 'custody_log_drain_failure'):
                collector.join(2)
            peer.join()
            self.assertEqual(collector.lifecycle()['rc'], 2)
        # Match P09's reader fixture seam; real bytes, watches and all five
        # lifetimes remain exercised. StrictLogRetentionTest owns kernel proof.
        retention = patch.object(log_fixture.logs.FileTrack, 'retained', return_value=True)
        retention.start()
        self.addCleanup(retention.stop)
        for round in range(5):
            with tempfile.TemporaryDirectory() as root:
                logs = log_fixture.Fixture(root)
                logs.capture.start(logs.roster)
                for path in logs.file_paths():
                    logs.append(path, ('collected-round-%d\n' % round).encode())
                peer = Peer()
                collector = collect.Collector({SOURCE: (peer.consumer, IDENTITY)}, logs=logs.capture)
                try:
                    seal = collector.seal('logs-' + str(round))
                    self.assertTrue(seal['logs']['complete'])
                    self.assertEqual(len(seal['logs']['positions']), 15)
                    file_rows = [log_fixture.logs.raw_document(p['raw'])['snapshot']
                                 for p in seal['logs']['positions']]
                    file_rows = [row for row in file_rows if row['kind'] == 'file']
                    self.assertTrue(file_rows, 'log collection must contain real file raw')
                    for row in file_rows:
                        for part in row['parts']:
                            raw = log_fixture.logs.file_bytes(part['segments'][0])
                            self.assertIn(('collected-round-%d\n' % round).encode(), raw)
                    collector.close()
                    collector.join(2)
                    self.assertEqual(logs.capture.sources, [])
                    self.assertEqual(collector.lifecycle()['rc'], 0)
                finally:
                    collector.close()
                    try:
                        collector.join(2)
                    finally:
                        peer.join()
        self.assertEqual(set(os.listdir('/proc/self/fd')), before)

    # origin: S10 internal review FIX-FULLSTACK2 F2
    @patch.object(log_fixture.logs.FileTrack, 'retained', lambda _track: True)
    def test_log_join_error_cannot_be_hidden_by_custody_flush(self):
        # Only the reader fixture certificate is supplied; Join must still fail.
        with tempfile.TemporaryDirectory() as root:
            logs = log_fixture.Fixture(root)
            logs.capture.start(logs.roster)
            collector, _ = self.make_collector(logs=logs.capture)
            collector.seal('logs-must-join')
            collector.close()
            with patch.object(logs.capture, 'join', side_effect=log_fixture.logs.SetupError('broken join')):
                with self.assertRaisesRegex(collect.SetupError, 'custody_log_join_failure'):
                    collector.join(2)
            self.assertEqual(collector.lifecycle()['rc'], 2)

    def test_runtime_consumer_keeps_unknowns_and_checks_actual_stamp_and_freshness(self):
        nonce = bytes(range(12))
        expected = dict(process=dict(pid=42, start_ticks=77, boot_id='boot', instance_id='instance',
                                     pidns='pid:[11]'), generation=3, cut_id='cut', scope_digest='scope',
                        query_nonce=nonce.hex())
        data = struct.pack('<H', 0x7fc0)
        for text in ('boot', 'instance', 'pid:[11]', 'cut', 'scope'):
            raw = text.encode()
            data += struct.pack('<H', len(raw)) + raw
        values = [1, 42, 77, 3, 9, 12, *([0] * 9), 1]
        row = dict(kind='runtime-sample', domain='agent', values=values, data=data,
                   nonce=nonce, digest=IDENTITY, at=150)
        barrier = dict(query_nonce=nonce.hex(), started_ns=100, finished_ns=200)
        sample = collect.consume_runtime(row, expected, barrier)
        self.assertEqual(sample['goroutines'], 9)
        self.assertEqual(sample['fds'], 12)
        self.assertTrue(all(v is None for v in sample['counters'].values()))
        self.assertFalse(sample['complete'], 'unknown owners must never become known zero')
        for defect in ('unknown-identity', 'generation', 'before-request', 'nonce', 'trailing', 'kind'):
            broken = dict(row)
            broken['values'] = list(values)
            if defect == 'unknown-identity':
                broken['data'] = struct.pack('<H', 1) + data[2:]
            elif defect == 'generation':
                broken['values'][3] += 1
            elif defect == 'before-request':
                broken['at'] = 99
            elif defect == 'nonce':
                broken['nonce'] = bytes(12)
            elif defect == 'trailing':
                broken['data'] += b'x'
            elif defect == 'kind':
                broken['domain'] = 'broker'
            with self.subTest(defect=defect):
                with self.assertRaises(collect.SetupError):
                    collect.consume_runtime(broken, expected, barrier)

    def test_broker_peer_current_stamp_and_full_response_are_independent(self):
        expected = dict(process=dict(pid=os.getpid(), key=dict(uid=os.getuid())),
                        query_nonce='fresh-query', generation=7, cut_id='cut-7')
        runtime = dict(schema='admin_runtime', schema_version=2, goroutines=321, threads=999,
                       open_fds=17, broker_image=dict(generation=7, cut_id='cut-7', running_sha256='a' * 64))
        def read(native, stamp=expected, changed=False, advance=False):
            with tempfile.TemporaryDirectory() as root:
                path = Path(root) / 'admin.sock'
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
                    server.bind(str(path))
                    server.listen(1)
                    failures = []
                    def serve():
                        try:
                            with server.accept()[0] as conn:
                                request = conn.recv(4096)
                                if request:
                                    self.assertEqual(request, b'{"op":"runtime"}\n')
                                    conn.sendall(native)
                        except BaseException as error:
                            failures.append(error)
                    worker = threading.Thread(target=serve)
                    worker.start()
                    calls = []
                    def query(nonce):
                        calls.append(nonce)
                        if advance:
                            return dict(stamp, sequence=len(calls), at_boot_ns=len(calls))
                        return dict(stamp, generation=8) if changed and len(calls) > 1 else stamp
                    try:
                        return collect.broker_runtime(path, stamp, query)
                    finally:
                        worker.join(2)
                        self.assertFalse(worker.is_alive())
                        self.assertFalse(failures)
        raw = json.dumps(dict(runtime=runtime), indent=2).encode() + b'\n'
        receipt = read(raw)
        self.assertEqual(receipt['response'], raw)
        self.assertEqual(receipt['peer_pid'], os.getpid())
        self.assertEqual(json.loads(receipt['response'])['runtime']['goroutines'], 321)
        self.assertLessEqual(receipt['sent_ns'], receipt['received_ns'])
        fresh = read(raw, advance=True)
        self.assertEqual(fresh['before']['sequence'], 1)
        self.assertEqual(fresh['after']['sequence'], 2)
        for defect in ('fd-unknown', 'fd-missing', 'image-missing', 'old-generation', 'changed-stamp', 'wrong-peer', 'invalid-sha'):
            broken = json.loads(raw)
            stamp = expected
            if defect == 'fd-unknown':
                broken['runtime']['open_fds'] = -1
            elif defect == 'fd-missing':
                del broken['runtime']['open_fds']
            elif defect == 'image-missing':
                del broken['runtime']['broker_image']
            elif defect == 'old-generation':
                broken['runtime']['broker_image']['generation'] -= 1
            elif defect == 'wrong-peer':
                stamp = dict(expected, process=dict(pid=os.getpid(), key=dict(uid=os.getuid() + 1)))
            elif defect == 'invalid-sha':
                broken['runtime']['broker_image']['running_sha256'] = 'z' * 64
            with self.subTest(defect=defect):
                with self.assertRaises(collect.SetupError):
                    read(json.dumps(broken).encode() + b'\n', stamp, defect == 'changed-stamp')

    def test_constructor_rejects_invalid_limits_and_shared_endpoint(self):
        left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        duplicate = socket.socket(fileno=os.dup(left.fileno()))
        try:
            for sources, limits in [({}, {}), ({SOURCE: (left, IDENTITY)}, {'byte_limit': collect.MAX_BYTES + 1}),
                                    ({SOURCE: (left, IDENTITY)}, {'fd_limit': collect.MAX_FDS + 1}),
                                    ({'a': (left, IDENTITY), 'b': (left, IDENTITY)}, {}),
                                    ({'a': (left, IDENTITY), 'b': (duplicate, IDENTITY)}, {})]:
                with self.subTest(limits=limits):
                    with self.assertRaises(collect.SetupError):
                        collect.Collector(sources, **limits)
                    self.assertGreaterEqual(left.fileno(), 0, 'partial validation must not consume caller FD')
        finally:
            left.close()
            right.close()
            duplicate.close()

    def test_broker_without_current_query_is_pending(self):
        with self.assertRaisesRegex(collect.PendingObservation, 'pending P18'):
            collect.broker_runtime('/unused/hermetic-admin.sock', {}, None)


if __name__ == '__main__':
    unittest.main()
