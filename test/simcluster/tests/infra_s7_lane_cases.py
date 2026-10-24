"""S7 original-event projection controls; no transfer or deployment is launched."""
import copy
import importlib
import json
import os
import socket
import sys
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'drills/lib'))
import hot_update_infra_probe as probe


class S7LaneTests(unittest.TestCase):
    def setUp(self):
        self.lanes = importlib.import_module('infra_s7_lanes')
        ctl = dict(pid=91, process_start=8, boot_id='boot', running_sha='ab' * 32)
        self.original = dict(cohort='small-push', transfer_id='original', entry_nonce='entry', object_version='NUID',
            ctl_producer='ctl', ctl=dict(PID=91, StartTicks=8, BootID='boot', ExeSHA256='ab' * 32))
        event = dict(schema=1, kind='append', producer='ctl', transfer_id='original', entry_nonce='entry',
            object_version='NUID', pid=91, process_start=8, boot_id='boot', generation=1, cut_id='',
            leg='leg1', phase='sending', event_seq=1, boottime_ns=100, actual_ack_seq=7)
        self.snapshot = dict(setup=[], failures=[], processes={'ctl': ctl}, events=[event], manifest_sha256='cd' * 32)

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-4 original phase binding
    def test_s7_lane_binding_comes_from_one_original_cohort_and_custody(self):
        self.assertEqual(self.lanes.binding([self.original], self.snapshot, 'small-push'), {'transfer_id': 'original'})
        for defect in ('missing', 'duplicate', 'nuid', 'ctl', 'producer', 'setup'):
            originals, snapshot = copy.deepcopy([self.original]), copy.deepcopy(self.snapshot)
            if defect == 'missing': originals = []
            if defect == 'duplicate': originals += copy.deepcopy(originals)
            if defect == 'nuid': snapshot['events'][0]['object_version'] = 'replacement'
            if defect == 'ctl': snapshot['processes']['ctl']['process_start'] += 1
            if defect == 'producer': snapshot['events'][0]['producer'] = 'foreign'
            if defect == 'setup': snapshot['setup'] = ['poisoned']
            with self.subTest(defect=defect), self.assertRaises(ValueError):
                self.lanes.binding(originals, snapshot, 'small-push')

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-4 observed lane events
    def test_s7_lane_projection_preserves_raw_events_and_requires_a_real_cut(self):
        self.snapshot['events'].append(dict(self.snapshot['events'][0], event_seq=2, boottime_ns=300))
        original = copy.deepcopy(self.snapshot)
        row = self.lanes.project('writer_lost_ack', 'hop-0', 200, self.original, self.snapshot,
                                dict(sent=True, effect=True, reply_cut=True))
        self.assertEqual(row['events'], self.snapshot['events'])
        self.assertEqual(row['hop'], 'hop-0')
        self.assertEqual(self.snapshot, original)
        for stamp, proof in ((99, dict(sent=True, effect=True, reply_cut=True)),
                             (301, dict(sent=True, effect=True, reply_cut=True)),
                             (200, dict(sent=True, effect=True, reply_cut=False))):
            with self.subTest(stamp=stamp, proof=proof), self.assertRaises(ValueError):
                self.lanes.project('writer_lost_ack', 'hop-0', stamp, self.original, self.snapshot, proof)

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-4 phase non-vacuity
    def test_s7_lanes_require_partial_sync_durable_rename_and_native_terminal(self):
        self.snapshot['events'].append(dict(self.snapshot['events'][0], event_seq=2, boottime_ns=300))
        cases = [('reader_partial_sync', [dict(kind='synced', durable_offset=1, fetched_offset=2)]),
                 ('commit_rename', [dict(kind='primitive_exit', outcome='landed'),
                                    dict(kind='directory_sync', parent_synced=True)]),
                 ('terminal_courier', [dict(kind='history', native_outcome=0)])]
        for lane, changes in cases:
            snapshot = copy.deepcopy(self.snapshot)
            for i, change in enumerate(changes):
                snapshot['events'].append(dict(snapshot['events'][0], **dict(change, event_seq=3 + i, boottime_ns=150)))
            with self.subTest(lane=lane):
                result = self.lanes.project(lane, 'hop-0', 200, self.original, snapshot)
                self.assertEqual(result['events'], snapshot['events'])
                with self.assertRaises(ValueError):
                    self.lanes.project(lane, 'hop-0', 200, self.original, self.snapshot)
                damaged = copy.deepcopy(snapshot)
                if lane == 'reader_partial_sync': damaged['events'][-1]['durable_offset'] = 2
                elif lane == 'commit_rename': damaged['events'][-1]['parent_synced'] = False
                else: damaged['events'][-1]['native_outcome'] = 23
                with self.assertRaises(ValueError):
                    self.lanes.project(lane, 'hop-0', 200, self.original, damaged)

    # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-4 terminal owner custody
    def test_s7_terminal_export_uses_ready_identity_after_the_original_ctl_exits(self):
        import infra_transfer_fixture as adapter
        import transfer_fixture as upstream
        import infra_composer_contract as contract
        import hot_update_infra_workload as workload
        with tempfile.TemporaryDirectory(prefix='infra-terminal-lane-') as temporary:
            root = Path(temporary)
            manifest, evidence = adapter.paths(dict(evidence=str(root)))
            evidence.mkdir(mode=0o700)
            manifest.write_text('{}')
            originals = []
            snapshot = dict(setup=[], failures=[], processes={}, events=[], manifest_sha256=contract.sha(manifest))
            for index, cohort in enumerate(probe.COHORTS):
                label = 'ctl-' + str(index)
                original = dict(self.original, cohort=cohort, transfer_id='original-' + str(index),
                                ctl_producer=label, ctl=dict(self.original['ctl'], PID=91 + index))
                originals.append(original)
                snapshot['processes'][label] = dict(self.snapshot['processes']['ctl'], pid=91 + index)
                snapshot['events'].append(dict(self.snapshot['events'][0], producer=label, pid=91 + index,
                                               transfer_id=original['transfer_id']))
            (evidence / 'snapshot.json').write_text(json.dumps(snapshot))
            ready = dict(schema='infra-cohort-v1', phase='ready', producer='composer', transfers=originals,
                s7_manifest_sha256=contract.sha(manifest), s7=dict(manifest=str(manifest), evidence=str(evidence)))
            (root / 'ready.json').write_text(json.dumps(ready))
            response = dict(native_rc=0, snapshot=str(evidence / 'snapshot.json'))
            with mock.patch.object(adapter, 'custody_call', return_value=response) as observed, \
                 mock.patch.object(adapter, 'call', return_value=response), \
                 mock.patch.object(upstream, 'read_manifest', return_value=dict(transfers=[self.original])) as reread, \
                 mock.patch.object(workload, 'process_identity', side_effect=FileNotFoundError('original ctl reaped')) as pid_read:
                actual, result = self.lanes.read_originals(dict(evidence=str(root)))
                self.assertEqual(actual, originals)
                self.assertEqual(result, snapshot)
                observed.assert_called_once_with(manifest, evidence, 'sample')
                reread.assert_not_called()
                pid_read.assert_not_called()

    # origin: hot-update S9 plan §9.1 / L22; S9 INSTRUMENTS-4 original custodian protocol
    def test_terminal_custody_call_binds_startup_bytes_and_preserves_native_failure(self):
        import infra_transfer_fixture as adapter
        import infra_composer_contract as contract
        before = len(os.listdir('/proc/self/fd'))
        for round in range(5):
            with tempfile.TemporaryDirectory(prefix='infra-custody-call-') as temporary:
                root = Path(temporary)
                manifest = root / 'manifest.json'; manifest.write_text('{}')
                with mock.patch.object(adapter, 'unix_call') as connect:
                    with self.assertRaisesRegex(ValueError, 'infra_manifest_changed'):
                        adapter.custody_call(manifest, root, 'sample', '00' * 32)
                    connect.assert_not_called()
                received = []
                with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as server:
                    adapter.unix_call(server, 'bind', root)
                    server.listen(1); server.settimeout(2)
                    def serve():
                        with server.accept()[0] as peer:
                            received.append(json.loads(peer.recv(65536)))
                            peer.send(json.dumps(dict(native_rc=23, snapshot='native-failure')).encode())
                    thread = threading.Thread(target=serve)
                    thread.start()
                    try:
                        value = adapter.custody_call(manifest, root, 'sample')
                        self.assertEqual(value, dict(native_rc=23, snapshot='native-failure'))
                    finally:
                        thread.join(timeout=3)
                        self.assertFalse(thread.is_alive())
                self.assertEqual(received, [dict(command='sample', manifest_sha256=contract.sha(manifest))])
        self.assertEqual(len(os.listdir('/proc/self/fd')), before)


if __name__ == '__main__':
    unittest.main()
