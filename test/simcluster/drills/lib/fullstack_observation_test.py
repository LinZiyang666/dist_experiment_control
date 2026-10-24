#!/usr/bin/env -S python3 -I -B
"""Hermetic adversarial controls for the private runtime-sample ABI.

These are explicitly synthetic binary codec controls. They never sign a
product rollout; the Go test separately exercises the real custody writer.
origin: hot-update S10 plan §5.3 / §10.2 P06
"""
import array
import hashlib
import os
from pathlib import Path
import socket
import struct
import sys
import unittest

# Isolated Python drops the script directory. Resolve only our owned sibling
# protocol path, so neither cwd nor PYTHONPATH determines the imported codec.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import flow_observation as observation


def control_event():
    payload = struct.pack('<H', 0x7fc0)
    for text in ('boot-1', 'instance-1', 'pid:[17]', '', '03' * 32):
        raw = text.encode()
        payload += struct.pack('<H', len(raw)) + raw
    return dict(kind='runtime-sample', domain='agent', endpoint=bytes(16), flow=bytes(16),
                actor=bytes(16), digest=hashlib.sha256(b'executing-image').digest(), nonce=b'\x01' + bytes(11),
                at=123456789, values=[1,123,567,7,9,11,0,0,0,0,0,0,0,0,0,1], data=payload)


def batch(event, sequence=1, poison=0, identity=b'i' * 32, incarnation=b'e' * 16):
    # Independent literal ABI encoder: no producer encoding routine defines
    # expected offsets, field order or length for this decoder test.
    raw = b'TDRILL1\0' + identity + incarnation + struct.pack('<QQI', sequence, poison, 1)
    for name in ('kind','domain'):
        value = event[name].encode('ascii')
        raw += struct.pack('B', len(value)) + value
    for name in ('endpoint','flow','actor','digest','nonce'):
        raw += event[name]
    raw += struct.pack('<17Q', event['at'], *event['values'])
    return raw + struct.pack('<I', len(event['data'])) + event['data']


def ack(event, sequence=1, poison=0):
    return b'TDSA' + event['nonce'] + bytes.fromhex('03' * 32) + struct.pack('<QQQ', event['values'][15], sequence, poison)


class FullstackObservationProtocolTest(unittest.TestCase):
    def decode_bytes(self, raw, retain=True):
        fd = observation.sealed(raw)
        try:
            return observation.decode(fd, retain_events=retain)
        finally:
            os.close(fd)

    def test_fixed_runtime_codec_in_both_decoder_paths(self):
        event = control_event()
        for retain in (True, False):
            identity, incarnation, sequence, events = self.decode_bytes(batch(event), retain)
            self.assertEqual((identity, incarnation, sequence), (b'i' * 32, b'e' * 16, 1))
            self.assertEqual(events, [event] if retain else [])
        decoded = observation.decode_runtime_sample(event)
        self.assertEqual(decoded['unknown'], 0x7fc0)
        self.assertEqual(decoded['values'], event['values'])
        self.assertEqual(decoded['pidns'], 'pid:[17]')
        self.assertEqual(decoded['instance_id'], 'instance-1')
        self.assertEqual(decoded['boot_id'], 'boot-1')
        self.assertEqual(decoded['scope_digest'], '03' * 32)
        self.assertEqual(decoded['exe_sha'], event['digest'])

    def test_missing_kind_domain_nonce_and_identity_poison_both_paths(self):
        changes = {
            'missing-kind': lambda e: e.update(kind=''),
            'unknown-kind': lambda e: e.update(kind='future-runtime'),
            'missing-domain': lambda e: e.update(domain=''),
            'foreign-domain': lambda e: e.update(domain='broker'),
            'known-wrong-domain': lambda e: e.update(domain='flow'),
            'missing-nonce': lambda e: e.update(nonce=bytes(12)),
            'missing-sha': lambda e: e.update(digest=bytes(32)),
            'foreign-flow': lambda e: e.update(flow=b'x' * 16),
            'zero-time': lambda e: e.update(at=0),
            'wrong-time-unit': lambda e: e.update(at=1 << 63),
            'zero-schema': lambda e: e['values'].__setitem__(0, 0),
            'unknown-schema': lambda e: e['values'].__setitem__(0, 2),
            'zero-pid': lambda e: e['values'].__setitem__(1, 0),
            'pid-overflow': lambda e: e['values'].__setitem__(1, 1 << 31),
            'zero-ticks': lambda e: e['values'].__setitem__(2, 0),
            'zero-goroutines': lambda e: e['values'].__setitem__(4, 0),
            'large-goroutines': lambda e: e['values'].__setitem__(4, 1 << 63),
            'zero-fds': lambda e: e['values'].__setitem__(5, 0),
            'zero-sequence': lambda e: e['values'].__setitem__(15, 0),
            'unknown-identity': lambda e: e.update(data=struct.pack('<H', 2) + e['data'][2:]),
            'unknown-schema-bit': lambda e: e.update(data=struct.pack('<H', 1) + e['data'][2:]),
            'unknown-sequence-bit': lambda e: e.update(data=struct.pack('<H', 1 << 15) + e['data'][2:]),
            'unknown-nonzero': lambda e: e['values'].__setitem__(6, 1),
            'unknown-cold-generation': lambda e: (e['values'].__setitem__(3, 0), e.update(data=struct.pack('<H', 0x7fc8) + e['data'][2:])),
            'large-known-counter': lambda e: (e['values'].__setitem__(6, 1 << 63), e.update(data=struct.pack('<H', 0x7f80) + e['data'][2:])),
            'short-unknown': lambda e: e.update(data=e['data'][:1]),
            'truncated-data': lambda e: e.update(data=e['data'][:3]),
            'trailing-data': lambda e: e.update(data=e['data'] + b'x'),
            'invalid-text': lambda e: e.update(data=e['data'][:4] + b'\xff' + e['data'][5:]),
            'empty-boot-id': lambda e: e.update(data=e['data'][:2] + b'\0\0' + e['data'][10:]),
            'length-overflow': lambda e: e.update(data=e['data'][:2] + b'\xff\xff' + e['data'][4:]),
            'length-over-256': lambda e: e.update(data=e['data'][:2] + struct.pack('<H', 257) + b'i' * 257 + e['data'][10:]),
            'unknown-claimed-zero': lambda e: e.update(data=struct.pack('<H', 0xffff) + e['data'][2:]),
        }
        for name, change in changes.items():
            for retain in (True, False):
                with self.subTest(control=name, retain=retain):
                    event = control_event()
                    change(event)
                    with self.assertRaises(observation.CustodyError):
                        self.decode_bytes(batch(event), retain)
                    with self.assertRaises(observation.CustodyError):
                        observation.decode_runtime_sample(event)

    def test_poison_is_sticky_after_queue_gap_for_both_paths(self):
        for retain in (True, False):
            records = observation.Records()
            try:
                records.add(observation.sealed(batch(control_event())), 'agent-one', retain)
                later = control_event()
                later['values'][15] = 2
                with self.assertRaisesRegex(observation.CustodyError, 'custody_sequence_gap'):
                    records.add(observation.sealed(batch(later, sequence=3)), 'agent-one', retain)
                self.assertTrue(records.broken)
                with self.assertRaises(observation.CustodyError):
                    records.rows('runtime-sample', 'agent-one')
            finally:
                records.close()

    def test_producer_poison_is_never_a_zero_sample(self):
        for retain in (True, False):
            for poison in (2,12,13,14,16,17,18,255):
                with self.subTest(retain=retain, poison=poison):
                    with self.assertRaises(observation.CustodyError):
                        self.decode_bytes(batch(control_event(), poison=poison), retain)

    def test_sample_gaps_and_identity_changes_poison_archive_only_cursors(self):
        changes = {
            'sample-gap': lambda e: e['values'].__setitem__(15, 3),
            'pid': lambda e: e['values'].__setitem__(1, 124),
            'ticks': lambda e: e['values'].__setitem__(2, 568),
            'generation': lambda e: e['values'].__setitem__(3, 8),
            'boot': lambda e: e.update(data=e['data'].replace(b'boot-1', b'boot-2')),
            'instance': lambda e: e.update(data=e['data'].replace(b'instance-1', b'instance-2')),
            'pidns': lambda e: e.update(data=e['data'].replace(b'pid:[17]', b'pid:[18]')),
            'sha': lambda e: e.update(digest=hashlib.sha256(b'other-image').digest()),
        }
        for retain in (True, False):
            for name, change in changes.items():
                with self.subTest(retain=retain, mutation=name):
                    records = observation.Records()
                    try:
                        records.add(observation.sealed(batch(control_event())), 'agent-one', retain)
                        event = control_event()
                        event['values'][15] = 2
                        change(event)
                        criterion = 'custody_runtime_sequence_gap' if name == 'sample-gap' else 'custody_runtime_identity_changed'
                        with self.assertRaisesRegex(observation.CustodyError, criterion):
                            records.add(observation.sealed(batch(event, sequence=2)), 'agent-one', retain)
                        self.assertTrue(records.broken)
                    finally:
                        records.close()

    def test_cold_generation_and_uint64_ordinals_remain_known(self):
        event = control_event()
        event['values'][3] = 0
        event['values'][15] = (1 << 64) - 1
        for retain in (True, False):
            _, _, _, rows = self.decode_bytes(batch(event), retain)
            self.assertEqual(rows, [event] if retain else [])

    def test_fresh_ack_requires_nonce_scope_ordinal_and_sealed_watermark(self):
        event = control_event()
        request = observation.runtime_sample_request(event['nonce'], bytes.fromhex('03' * 32))
        self.assertEqual(request, b'TDS1' + event['nonce'] + bytes.fromhex('03' * 32))
        packet = ack(event)
        self.assertEqual(len(packet), 72)
        sample = observation.validate_runtime_sample_ack(packet, event, 1, event['at'] - 1)
        self.assertEqual(sample['nonce'], event['nonce'])
        for offset in (0,4,16,48,56,64):
            changed = bytearray(packet)
            # Keep altered ordinals positive. Flipping 1 to 0 lets the stale
            # fence hide a missing ACK equality check during mutation testing.
            # origin: hot-update S10 plan §10.2 P06 / ACK ordinal mutation
            changed[offset] ^= 2 if offset in (48,56) else 1
            with self.subTest(offset=offset), self.assertRaises(observation.CustodyError):
                observation.validate_runtime_sample_ack(bytes(changed), event, 1, event['at'] - 1)
        for wrong in (packet[:-1], packet + b'\0', b'TDOK', ack(event, sequence=0)):
            with self.assertRaises(observation.CustodyError):
                observation.validate_runtime_sample_ack(wrong, event, 1, event['at'] - 1)
        with self.assertRaisesRegex(observation.CustodyError, 'custody_runtime_stale'):
            observation.validate_runtime_sample_ack(packet, event, 1, event['at'])
        with self.assertRaisesRegex(observation.CustodyError, 'custody_runtime_stale'):
            observation.validate_runtime_sample_ack(packet, event, 1, event['at'] - 1, after_sequence=1)
        for nonce, scope in ((bytes(12), b'x' * 32), (b'x' * 12, bytes(32)), (b'x', b'x' * 32)):
            with self.assertRaises(observation.CustodyError):
                observation.runtime_sample_request(nonce, scope)

    def test_five_sealed_batch_and_socket_lifecycles_close_exactly(self):
        before = set(os.listdir('/proc/self/fd'))
        for _ in range(5):
            records = observation.Records()
            sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            exported = observation.sealed(batch(control_event()))
            try:
                rights = array.array('i', [exported])
                sender.sendmsg([b'TDC1'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)])
                packet, fds = observation.receive(receiver)
                self.assertEqual((packet, len(fds)), (b'TDC1', 1))
                self.assertFalse(os.get_inheritable(fds[0]))
                records.add(fds[0], 'agent-one')
                self.assertEqual(len(records.rows('runtime-sample')), 1)
                records.compact()
                self.assertEqual(len(records.frames), 1)
                self.assertEqual(len(records.fds), 1)
            finally:
                records.close()
                sender.close()
                receiver.close()
                os.close(exported)
            self.assertEqual(set(os.listdir('/proc/self/fd')), before)

    def test_truncated_unsealed_and_replayed_incarnation_are_rejected(self):
        raw = batch(control_event())
        for retain in (True, False):
            for changed in (raw[:75], raw[:-1], raw + b'x'):
                with self.assertRaises(observation.CustodyError):
                    self.decode_bytes(changed, retain)
        records = observation.Records()
        try:
            records.add(observation.sealed(raw), 'agent-one')
            records.add(observation.sealed(batch(control_event(), incarnation=b'f' * 16)), 'agent-one')
            with self.assertRaisesRegex(observation.CustodyError, 'custody_replayed_incarnation'):
                records.add(observation.sealed(batch(control_event(), sequence=2)), 'agent-one')
        finally:
            records.close()
        fd = os.memfd_create('unsealed-control', os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        try:
            os.write(fd, raw)
            with self.assertRaises(observation.CustodyError):
                observation.decode(fd)
        finally:
            os.close(fd)


if __name__ == '__main__':
    unittest.main()
