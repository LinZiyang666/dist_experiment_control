"""Hermetic original infrastructure custody controls; no deployment is invoked."""
import copy
import array
import json
import os
import socket
import subprocess
import tempfile
import threading
import time
from unittest import mock
import hashlib
from pathlib import Path
import struct
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'drills/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import flow_observation as flow


class InfrastructureExportTests(unittest.TestCase):
    # origin: S9 internal review NC-EXPORTS handoff instrument decoding
    def test_infrastructure_kinds_are_retained_by_archive_only_decoder(self):
        self.assertTrue({'infra-attachment', 'infra-prefix'} <= flow.KINDS,
                        'new infrastructure exports are rejected by the closed custody vocabulary')
        import infra_native_custody as infra
        for kind, domain in infra.DOMAINS.items():
            raw = b'{"row":"' + b'x' * 33000 + b'"}'
            fd = flow.sealed(batch(kind, domain, raw[:32768], total=len(raw), digest=hashlib.sha256(raw).digest()))
            self.addCleanup(lambda fd=fd: __import__('os').close(fd))
            rows = flow.decode(fd, retain_events=False)[3]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['data'], raw[:32768])

    # origin: S9 internal review NC-EXPORTS handoff fragment identity
    def test_infrastructure_fragments_require_contiguous_actor_step_and_digest(self):
        import infra_native_custody as infra
        data = b'{"transfer_id":"original","at_ns":10,"before_repair_ns":20,"expected":[],"actual":[]}'
        rows = fragments('infra-prefix', data)
        decoded = infra.reassemble(rows)
        self.assertEqual(decoded[0]['row']['transfer_id'], 'original')
        other = fragments('infra-prefix', data)
        for row in other: row['actor'] = b'b' * 16
        self.assertEqual(len(infra.reassemble([rows[0],other[0],rows[1],other[1]])),2)
        for defect in ('gap', 'overlap', 'actor', 'step', 'digest', 'mixed_digest', 'total', 'domain', 'reserved', 'truncated'):
            changed = copy.deepcopy(rows)
            if defect == 'gap': changed[1]['values'][0] += 1
            if defect == 'overlap': changed[1]['values'][0] -= 1
            if defect == 'actor': changed[1]['actor'] = b'b' * 16
            if defect == 'step': changed[1]['values'][2] += 1
            if defect == 'digest':
                for row in changed: row['digest'] = b'x' * 32
            if defect == 'mixed_digest': changed[1]['digest'] = b'x' * 32
            if defect == 'total': changed[1]['values'][1] += 1
            if defect == 'domain': changed[1]['domain'] = 'infra'
            if defect == 'reserved': changed[0]['values'][3] = 1
            if defect == 'truncated': changed.pop()
            with self.subTest(defect=defect), self.assertRaises(ValueError): infra.reassemble(changed)

    # origin: S9 internal review NC-EXPORTS handoff TDIA roster binding
    def test_infrastructure_ack_requires_fresh_sealed_complete_roster(self):
        import infra_native_custody as infra
        nonce = b'n' * 12
        event = fragments('infra-attachment', b'{"Participant":"' + (b'a' * 16).hex().encode() + b'"}')[0]
        event['data'] = b'{"Participant":"' + (b'a' * 16).hex().encode() + b'"}'
        event['values'][1] = len(event['data'])
        event['digest'] = hashlib.sha256(event['data']).digest()
        ack = b'TDIA' + nonce + struct.pack('<QQ', 5, 0)
        result = infra.validate_ack(ack, nonce, 5, 3, [event])
        self.assertEqual(len(result), 1)
        for packet, current, before, rows in ((ack, 4, 3, [event]), (ack, 5, 5, [event]),
            (ack, 5, 3, []), (ack[:4] + b'z'*12 + ack[16:], 5, 3, [event]),
            (ack[:-8] + struct.pack('<Q', 1), 5, 3, [event])):
            with self.subTest(packet=packet), self.assertRaises(ValueError):
                infra.validate_ack(packet, nonce, current, before, rows)

    # origin: S9 internal review NC-EXPORTS handoff original prefix selection
    def test_prefix_reader_keeps_first_original_loss_and_maps_both_clocks(self):
        import infra_native_custody as infra
        def receipt(actual, at):
            return dict(kind='infra-prefix', actor=b't'*16, step=7, at=at,
                row=dict(transfer_id='original', at_ns=at-2, before_repair_ns=at-1,
                    expected=[dict(index=0, length=1, sha256='a'*64)], actual=actual))
        repaired = [dict(index=0, length=1, sha256='a'*64)]
        rows = [receipt([], 50), receipt(repaired, 60)]
        actual = infra.prefixes(rows, (b't'*16).hex(), 7, ['original'], 1000)
        self.assertEqual(actual[0]['actual'], [])
        self.assertEqual((actual[0]['at_ns'], actual[0]['before_repair_ns']), (1048, 1049))
        for tx, step, ids in (((b'x'*16).hex(), 7, ['original']), ((b't'*16).hex(), 8, ['original']),
                              ((b't'*16).hex(), 7, ['object-NUID'])):
            with self.assertRaises(ValueError): infra.prefixes(rows, tx, step, ids, 1000)

    # origin: S9 internal review NC-EXPORTS handoff retained seqpacket protocol
    def test_infrastructure_keeper_drains_original_sealed_rows_before_ack(self):
        import infra_native_custody as infra
        from hot_update_infra_probe_fixtures import attachment
        original = attachment(1)
        original['Participant'] = (b'a' * 16).hex()
        payload = json.dumps(original).encode()
        producer, consumer = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.addCleanup(producer.close)
        self.addCleanup(consumer.close)
        failures = []
        def emit():
            try:
                request = producer.recv(128)
                self.assertEqual(request[:4], b'TDI1')
                self.assertEqual(len(request), 24)
                fd = flow.sealed(batch('infra-attachment', 'infra', payload))
                try:
                    producer.sendmsg([b'TDC1'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [fd]))])
                finally: os.close(fd)
                producer.send(b'TDIA' + request[4:16] + struct.pack('<QQ', 1, 0))
            except BaseException as error: failures.append(error)
        supervisor = os.environ['SIM_UPGRADE_SUPERVISOR']
        with tempfile.TemporaryDirectory(prefix='infra-sealed-export-') as directory:
            child = subprocess.Popen([supervisor, '--', sys.executable, '-I', '-B', flow.__file__, 'keeper',
                '--root', directory, '--node', 'agt1', '--fd', '0'],
                stdin=consumer, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            thread = threading.Thread(target=emit)
            try:
                due = time.monotonic() + 5
                while not (Path(directory) / 'agt1.sock').exists() and time.monotonic() < due:
                    self.assertIsNone(child.poll(), child.stderr.read() if child.poll() is not None else None)
                    time.sleep(.01)
                thread.start()
                value = infra.collect(directory, 'agt1')
                self.assertEqual(value['roster'][0]['row'], original)
                self.assertEqual(value['rows'][0]['row'], original)
                self.assertGreater(value['boot_ns'], 0)
                thread.join(5)
                self.assertFalse(thread.is_alive())
                self.assertEqual(failures, [])
            finally:
                producer.shutdown(socket.SHUT_RDWR)
                if thread.ident is not None: thread.join(5)
                child.terminate()
                child.communicate(timeout=15)

    # origin: S9 internal review NC-EXPORTS handoff native-deployment readers
    def test_native_readers_select_original_roster_and_actual_journal_step(self):
        import infra_native_exports as exports
        from hot_update_infra_probe_fixtures import attachment
        original = attachment(1)
        participant = (b'a' * 16).hex()
        original['Participant'] = participant
        source = dict(node='ctl1', ingress_node='brk1', reader=dict(custody=dict(root='/private/custody', source='ctl1')))
        with mock.patch.object(exports, 'remote_read', return_value=(dict(attachments=[original]), 10)):
            self.assertEqual(exports.attachment({}, {}, source, participant), original)
            with self.assertRaisesRegex(ValueError, 'original_participant_missing'):
                exports.attachment({}, {}, source, (b'b' * 16).hex())
        python = str(Path(sys.executable).resolve())
        import infra_composer_contract as contract
        config = dict(repair_fence=dict(custody=dict(node='brk1', root='/private/custody', source='brk1'),
            binding=dict(argv=[python], sha256=contract.sha(python))))
        prefix = dict(transfer_id='original', at_ns=50, before_repair_ns=60,
            expected=[dict(index=0,length=1,sha256='a'*64)],actual=[])
        value = dict(prefixes=[dict(tx=(b't'*16).hex(),step=7,at_ns=70,row=prefix)])
        with mock.patch('infra_native.invoke', return_value=(0, json.dumps(dict(tx=(b't'*16).hex(),step=7,revision=3)).encode())), \
             mock.patch.object(exports, 'remote_read', return_value=(value, 1000)):
            result = exports.repair({},config,dict(evidence='/private/evidence'),'hop-0','verifying',['original'])
            self.assertEqual(result[0]['at_ns'],1050)
            self.assertEqual(result[0]['before_repair_ns'],1060)
            self.assertEqual(result[0]['actual'],[])
        with mock.patch('infra_native.invoke', return_value=(0, json.dumps(dict(tx=(b't'*16).hex(),step=8,revision=3)).encode())), \
             mock.patch.object(exports, 'remote_read', return_value=(value,1000)):
            with self.assertRaisesRegex(ValueError,'prefix_requiredset'):
                exports.repair({},config,dict(evidence='/private/evidence'),'hop-0','verifying',['original'])

    # origin: S9 internal review NC-EXPORTS handoff remote executable/clock custody
    def test_native_custody_reader_verifies_remote_bytes_and_clock_mapping(self):
        import infra_native_exports as exports
        import infra_native as native
        import infra_composer_contract as contract
        import infra_owner_plan as plans
        names = ('infra_native_exports.py', 'infra_native_custody.py', 'flow_observation.py',
                 'hot_update_infra_probe.py', 'transfer_observation.py')
        sums = ('\n'.join(contract.sha(plans.LIB / name) + '  ' + str(plans.LIB / name) for name in names) + '\n').encode()
        payload = json.dumps(dict(attachments=[], prefixes=[], boot_ns=100)).encode()
        source = dict(root='/private/custody', source='agt1')
        with mock.patch.object(native, 'remote', side_effect=[sums, payload]), \
             mock.patch.object(exports.time, 'monotonic_ns', side_effect=[1000,1200]):
            row, offset = exports.remote_read({}, {}, source, 'agt1')
            self.assertEqual(offset,1100)
            self.assertEqual(row['boot_ns'],100)
        with mock.patch.object(native, 'remote', return_value=sums.replace(sums[:64],b'0'*64,1)):
            with self.assertRaisesRegex(ValueError, 'target_custody_reader_sha'):
                exports.remote_read({}, {}, source, 'agt1')
        for value in (dict(custody=dict(root='/private/../foreign',source='agt1')),
                      dict(custody=dict(root='/private/custody',source='../foreign')),
                      dict(custody=dict(root='/private/custody',source='agt1'),expected={})): 
            with self.assertRaises(ValueError): exports.declaration(value)

    # origin: S9 internal review NC-EXPORTS handoff preserve original prefix loss
    def test_native_sample_preserves_an_empty_actual_prefix_for_the_oracle(self):
        import infra_native_targets as targets
        import infra_native_exports as exports
        import infra_native as native
        prefixes = [dict(transfer_id='original'+str(i),at_ns=100,before_repair_ns=120,
            expected=[dict(index=0,length=1,sha256='a'*64)],actual=[]) for i in range(4)]
        metadata = dict(before={},current={'process':'executing'},temporary=[],census={})
        config = dict(repair_fence={'custody':{}},phase_reader={})
        context = dict(hops=[('nats','upgrade','brk1','candidate')])
        with mock.patch.object(targets,'s7_binding',return_value=dict(transfers=[dict(transfer_id=p['transfer_id']) for p in prefixes])), \
             mock.patch.object(exports,'repair',return_value=prefixes), \
             mock.patch.object(targets,'attachments',return_value=({},{})), \
             mock.patch.object(native,'invoke',return_value=(0,json.dumps(metadata).encode())), \
             mock.patch.object(native,'node_observation',return_value=dict(server=metadata['current'])), \
             mock.patch.object(targets,'business',return_value={}):
            result = targets.observation({},config,context,'readers.sample',['hop-0','verifying'],10)
            self.assertEqual(result['prefix_reads'],prefixes)
            self.assertTrue(all(row['actual']==[] for row in result['prefix_reads']))


def fragments(kind, data):
    split = len(data) // 2
    return [dict(kind=kind, domain='store' if kind == 'infra-prefix' else 'infra', endpoint=bytes(16),
        flow=bytes(16), nonce=bytes(12), actor=b'a'*16, digest=hashlib.sha256(data).digest(), at=100,
        values=[offset, len(data), 7 if kind == 'infra-prefix' else 0] + [0]*13, data=data[offset:end])
        for offset, end in ((0, split), (split, len(data)))]


def batch(kind, domain, data, total=None, digest=None):
    values = [0, total or len(data), 1 if kind == 'infra-prefix' else 0] + [0]*13
    event = bytes([len(kind)]) + kind.encode() + bytes([len(domain)]) + domain.encode()
    event += bytes(32) + b'a'*16 + (digest or hashlib.sha256(data).digest()) + bytes(12)
    event += struct.pack('<17QI', 100, *values, len(data)) + data
    return b'TDRILL1\0' + b'i'*32 + b'c'*16 + struct.pack('<QQI', 1, 0, 1) + event


if __name__ == '__main__': unittest.main()
