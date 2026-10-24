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

class UpgradeFixtureOracle(unittest.TestCase):
    def setUp(self):
        self.row, self.originals, self.manifest = sealed_pair()

    def rejected(self, fn, reason=None, rc=None):
        with self.assertRaises(C.EvidenceError) as caught:
            fn()
        if reason:
            self.assertIn(reason, str(caught.exception))
        if rc:
            self.assertEqual(rc, caught.exception.rc)

    # origin: hot-update S8 plan §9.0 / M20.
    def test_original_set_cannot_shrink(self):
        self.row['resources'] = [x for x in self.row['resources'] if x['id'] != 'pty.0']
        self.rejected(lambda: C.validate_sample(self.row, self.originals, NONCE), 'original_resource_missing', 1)

    def test_original_seal_cannot_be_recalculated_from_survivors(self):
        self.originals['resources'].pop()
        self.rejected(lambda: C.validate_sample(self.row, self.originals, NONCE), 'original_set_seal_changed', 1)
        self.row, self.originals, _ = sealed_pair()
        self.originals['manifest_digest'] = 'f' * 64
        self.rejected(lambda: C.validate_sample(self.row, self.originals, NONCE), 'original_set_seal_changed', 1)

    # origin: hot-update S8 plan §9.0 / M19.
    def test_phase_event_cannot_replace_hit_interval(self):
        hit = self.row['hit']
        hit.update(known=True, begin_ns=110, end_ns=120, syscall='execve', errno=13,
                   mechanism='noexec', window_begin_ns=100, window_end_ns=130)
        self.row.update(hit_begin_ns=110, hit_end_ns=120, syscall='execve', errno=13)
        case = dict(armed_ns=100, released_ns=130, mechanism='noexec', syscall='execve', errno=13,
                    target=dict(owner=copy.deepcopy(OWNER), cut_id=CUT, resource_id=hit['resource_id']))
        C.validate_hit(self.row, case)
        self.row['cut_id'] = 'f'*32  # Reporter has already started current reverse.
        try: C.validate_hit(self.row, case)
        except C.EvidenceError as error: self.fail('archived hit was bound to the current reporter: '+str(error))
        hit['window_begin_ns'] = 121
        self.rejected(lambda: C.validate_hit(self.row, case), 'fault_hit_outside_interval', 1)

    def test_unhit_or_wrong_errno_stays_incomplete(self):
        case = dict(armed_ns=100, released_ns=130, mechanism='noexec', syscall='execve', errno=13,
                    target=dict(owner=copy.deepcopy(OWNER), cut_id=CUT, resource_id=self.row['hit']['resource_id']))
        self.row['hit'].update(known=False, begin_ns=110, end_ns=120, window_begin_ns=100, window_end_ns=130,
            syscall='execve', errno=13, mechanism='noexec')
        self.row.update(hit_begin_ns=110, hit_end_ns=120, syscall='execve', errno=13)
        self.rejected(lambda: C.validate_hit(self.row, case), 'fault_not_hit', 4)
        self.row['hit'].update(known=True, errno=0)
        self.row['errno'] = 0
        self.rejected(lambda: C.validate_hit(self.row, case), 'fault_mechanism_not_observed', 4)

    # origin: hot-update S8 plan §4.2 / §9.0 / M07.
    def test_each_receipt_rejects_another_cut(self):
        for field, kind in [('control_receipt', 'control_ready'), ('resources_receipt', 'resources_migrated'),
                            ('released_receipt', 'old_generation_released')]:
            with self.subTest(kind=kind):
                row = copy.deepcopy(self.row)
                try: C.validate_receipt(row, field, kind)
                except C.EvidenceError as error: self.fail('fresh Unix receipt was refused: '+str(error))
                row[field]['stamp']['attempt']['cut_id'] = 'f' * 32
                self.rejected(lambda: C.validate_receipt(row, field, kind), 'receipt_attempt_mismatch', 1)

    def test_each_unknown_receipt_rejects_zero_count(self):
        for field, kind in [('control_receipt', 'control_ready'), ('resources_receipt', 'resources_migrated'),
                            ('released_receipt', 'old_generation_released')]:
            for zero_count in (False, True):
                with self.subTest(kind=kind, zero_count=zero_count):
                    row = copy.deepcopy(self.row)
                    row[field]['known'] = False
                    if zero_count: row[field]['census']['resources'] = []
                    self.rejected(lambda: C.validate_receipt(row, field, kind), 'receipt_unknown', 1)

    def test_each_missing_receipt_is_setup_red(self):
        for field in ('control_receipt', 'resources_receipt', 'released_receipt'):
            with self.subTest(field=field):
                row = copy.deepcopy(self.row)
                del row[field]
                self.rejected(lambda: C.schema_check(row), 'schema_missing', 2)

    # origin: hot-update S8 plan §9.0 / M04 / M14.
    def test_new_admission_cannot_be_swallowed(self):
        extra = copy.deepcopy(self.row['resources'][-1])
        extra.update(id='late.prepare', domain='transfer_tracker', family='tracker')
        self.row['resources'].append(extra)
        self.row['admissions'] = contract_rows(dict(resources=[extra]))
        C.validate_sample(self.row, self.originals, NONCE, self.row['admissions'])
        self.row['resources'].pop()
        self.rejected(lambda: C.validate_sample(self.row, self.originals, NONCE), 'new_admission_swallowed', 1)

    def test_independent_admission_reader_catches_an_omitted_begin(self):
        admission = contract_rows(self.row)[0]
        self.rejected(lambda: C.validate_sample(self.row, self.originals, NONCE, [admission]),
                      'independent_admission_census_mismatch', 1)

    # origin: hot-update S8 plan §9.0. Environment recovery cannot reconnect
    # a business stream to compensate for loss of its original socket.
    def test_business_reconnect_cannot_compensate(self):
        self.row['resources'][0]['reconnects'] += 1
        self.rejected(lambda: C.validate_sample(self.row, self.originals, NONCE), 'reconnect_changed', 3)

    def test_equal_fd_numbers_cannot_prove_an_ofd(self):
        self.row['resources'][0]['ofd']['method'] = 'fd_number'
        self.rejected(lambda: C.schema_check(self.row), 'schema_enum', 2)
        self.row['resources'][0]['ofd']['method'] = 'not_applicable'
        self.rejected(lambda: C.validate_resources(self.row), 'kernel_ofd_proof_missing', 2)

    def test_same_inode_reopen_fails_original_ofd(self):
        self.row['resources'][0]['ofd']['same'] = False
        self.rejected(lambda: C.validate_sample(self.row, self.originals, NONCE), 'original_ofd_reopened', 3)

    def test_original_source_tmp_parent_and_nuid_cannot_change(self):
        transfer = next(row for row in self.row['resources'] if row['family'] == 'transfer')
        transfer['files'].pop()
        self.rejected(lambda: C.seal(self.row, C.digest(self.manifest)), 'source_tmp_parent_ofd_missing', 2)
        self.rejected(lambda: C.validate_sample(self.row, self.originals, NONCE), 'source_tmp_parent_ofd_missing', 2)
        self.row, _, _ = sealed_pair()
        transfer = next(row for row in self.row['resources'] if row['family'] == 'transfer')
        transfer['transfer']['object_nuid_hash'] = OTHER
        self.rejected(lambda: C.validate_sample(self.row, self.originals, NONCE), 'original_identity_or_reconnect_changed', 3)

    def test_actual_transfer_budget_and_four_original_ids_are_required(self):
        C.validate_transfer_budget(self.row, self.manifest['transfer_budget'])
        transfers = [row['transfer'] for row in self.row['resources'] if row['family'] == 'transfer']
        transfers[0]['budget_deadline_ns'] = self.row['observed_end_ns'] + 1
        self.rejected(lambda: C.validate_transfer_budget(self.row, self.manifest['transfer_budget']),
                      'actual_original_transfer_budget_too_short', 2)
        self.row, _, _ = sealed_pair()
        transfers = [row['transfer'] for row in self.row['resources'] if row['family'] == 'transfer']
        transfers[0]['business_id'] = transfers[1]['business_id']
        self.rejected(lambda: C.validate_transfer_budget(self.row, self.manifest['transfer_budget']),
                      'four_original_transfer_identities_missing', 2)

    def test_related_ofd_and_unknown_observations_fail_closed(self):
        for defect, reason, rc in [('complete', 'observation_unknown', 2), ('owner', 'resource_identity_unknown', 2),
                                  ('ofd', 'ofd_unknown', 2), ('file', 'source_tmp_parent_ofd_reopened', 3),
                                  ('transfer', 'original_tier_b_transfer_unknown', 2)]:
            row = copy.deepcopy(self.row)
            transfer = next(item for item in row['resources'] if item['family'] == 'transfer')
            if defect == 'complete': row['complete'] = False
            elif defect == 'owner': row['resources'][0]['known'] = False
            elif defect == 'ofd': row['resources'][0]['ofd']['known'] = False
            elif defect == 'file': transfer['files'][0]['proof']['same'] = False
            else: transfer['transfer']['known'] = False
            self.rejected(lambda: C.validate_resources(row), reason, rc)

    def test_measured_cut_without_current_receipts_stays_incomplete(self):
        self.rejected(lambda: C.validate_a6(self.row, self.originals, [self.row]),
                      'a6_measured_cut_has_no_completed_observation', 4)

    def test_receipt_evidence_cannot_be_reused_across_kinds(self):
        with tempfile.TemporaryDirectory(prefix='s8-receipt-') as temporary:
            root = Path(temporary)
            evidence(root, self.row, self.originals, self.manifest)
            latest_path = sorted((root / 'samples').glob('*.json'))[-1]
            latest = C.read(latest_path)
            latest['resources_receipt']['evidence_digest'] = latest['control_receipt']['evidence_digest']
            S.write_json(latest_path, latest)
            self.rejected(lambda: C.judge(root, self.manifest), 'completion_receipts_not_independent', 1)

    def test_publication_never_overwrites_an_existing_nonce(self):
        with tempfile.TemporaryDirectory(prefix='s8-publish-') as temporary:
            path = Path(temporary) / 'one.json'
            C.publish(path, dict(sequence=1))
            self.rejected(lambda: C.publish(path, dict(sequence=2)), 'evidence_already_published', 1)
            self.assertEqual(C.read(path), dict(sequence=1))

    def test_completion_rejects_wrong_producer_census_and_running_sha(self):
        row = copy.deepcopy(self.row)
        row['control_receipt']['stamp']['producer']['pid'] += 1
        row['control_receipt']['census']['stamp'] = copy.deepcopy(row['control_receipt']['stamp'])
        self.rejected(lambda: C.validate_receipt(row, 'control_receipt', 'control_ready'), 'receipt_producer_mismatch', 1)
        row = copy.deepcopy(self.row)
        row['control_receipt']['census']['known'] = False
        self.rejected(lambda: C.validate_receipt(row, 'control_receipt', 'control_ready'), 'receipt_census_unknown', 1)
        row = copy.deepcopy(self.row)
        row['running_sha'] = SHA
        self.rejected(lambda: C.validate_receipt(row, 'control_receipt', 'control_ready'), 'receipt_running_image_mismatch', 3)

    def test_later_resource_progress_cannot_regress(self):
        with tempfile.TemporaryDirectory(prefix='s8-regress-') as temporary:
            root = Path(temporary)
            evidence(root, self.row, self.originals, self.manifest)
            latest_path = sorted((root / 'samples').glob('*.json'))[-1]
            latest = C.read(latest_path)
            latest['resources'][0]['sequence'] -= 1
            S.write_json(latest_path, latest)
            self.rejected(lambda: C.judge(root, self.manifest), 'resource_progress_regressed', 3)

    def test_fresh_queries_can_share_an_unchanged_admission_sequence(self):
        with tempfile.TemporaryDirectory(prefix='s8-fresh-') as temporary:
            root = Path(temporary)
            evidence(root, self.row, self.originals, self.manifest)
            latest = C.read(sorted((root / 'samples').glob('*.json'))[-1])
            latest.update(nonce='d'*32, observed_begin_ns=latest['observed_begin_ns']+1000,
                          observed_end_ns=latest['observed_end_ns']+1000,
                          observed_begin_unix_ns=latest['observed_begin_unix_ns']+1000, observed_end_unix_ns=latest['observed_end_unix_ns']+1000)
            for field in ('control_receipt', 'resources_receipt', 'released_receipt'):
                stamp = latest[field]['stamp']
                stamp.update(nonce=latest['nonce'], at=latest['observed_begin_unix_ns']+50)
                latest[field]['census']['stamp'] = copy.deepcopy(stamp)
            C.publish(root / 'samples' / (latest['nonce']+'.json'), latest)
            C.publish(root / 'admissions' / (latest['nonce']+'.json'), dict(version=1, nonce=latest['nonce'], run_id=ID,
                sequence=latest['sequence'], known=True, complete=True, resources=[]))
            try: self.assertEqual(C.judge(root, self.manifest)['rc'], 0)
            except C.EvidenceError as error: self.fail('fresh query was refused: '+str(error))

    def test_schema_rejects_unknown_secret_duplicate_and_boolean_integer(self):
        C.schema_check(self.row)
        for mutant in ('unknown', 'boolean', 'version', 'missing'):
            with self.subTest(mutant=mutant):
                row = copy.deepcopy(self.row)
                if mutant == 'unknown': row['salt'] = 'private-material-must-not-leave-custody'
                elif mutant == 'boolean': row['pid'] = True
                elif mutant == 'version': row['version'] = 2
                else: del row['resources'][0]['gc_permit']
                self.rejected(lambda: C.schema_check(row), 'schema_', 2)
        self.rejected(lambda: C.decode(b'{"pid":1,"pid":2}'), 'duplicate_json_field', 2)

    def test_stale_nonce_and_late_old_worker_fail(self):
        self.rejected(lambda: C.validate_sample(self.row, self.originals, 'f' * 32), 'stale_observation_nonce', 1)
        self.row['released_receipt']['census']['old_owners'] = [OWNER]
        self.rejected(lambda: C.validate_receipt(self.row, 'released_receipt', 'old_generation_released'), 'old_owner_not_released', 3)

    def test_full_freeze_includes_slowest_domain(self):
        C.validate_a6(self.row, self.originals)
        self.row['metrics']['freezes'][1]['last_pump_ready_ns'] += 101_000_000
        self.rejected(lambda: C.validate_a6(self.row, self.originals), 'a6_full_freeze_exceeded', 3)

    def test_all_scheduled_slots_and_effects_count(self):
        self.row['metrics']['requests'].pop()
        self.rejected(lambda: C.validate_a6(self.row, self.originals), 'a6_scheduled_slot_missing', 1)
        self.row, _, _ = sealed_pair()
        self.row['metrics']['requests'][1]['durable_effects'] = 2
        self.rejected(lambda: C.validate_a6(self.row, self.originals), 'a6_duplicate_or_missing_effect', 3)

    def test_durable_transfer_progress_cannot_stall(self):
        points = self.row['metrics']['transfer_progress']
        points[1]['durable_bytes'] = points[0]['durable_bytes']
        self.rejected(lambda: C.validate_a6(self.row, self.originals), 'a6_original_progress_stalled', 3)

    def test_full_calibration_and_three_real_cuts_are_required(self):
        self.row['metrics']['calibrations'].pop()
        self.rejected(lambda: C.validate_a6(self.row, self.originals), 'a6_calibration_rounds_missing', 4)
        self.row, _, _ = sealed_pair()
        self.row['metrics']['freezes'].pop()
        self.rejected(lambda: C.validate_a6(self.row, self.originals), 'a6_three_cuts_missing', 4)

    def test_two_drains_preserve_exact_physical_history(self):
        row = drains(self.originals)[0]
        C.validate_drain(row, self.originals)
        row['history'][0]['physical_records'] = 3
        self.rejected(lambda: C.validate_drain(row, self.originals), 'drain_history_not_exact', 3)
        row = drains(self.originals)[0]
        row['joined'] = False
        self.rejected(lambda: C.validate_drain(row, self.originals), 'drain_ack_or_join_missing', 2)

    def test_judge_is_read_only_and_keeps_native_result(self):
        with tempfile.TemporaryDirectory(prefix='s8-oracle-') as temporary:
            root = Path(temporary)
            evidence(root, self.row, self.originals, self.manifest)
            before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in root.rglob('*') if path.is_file()}
            self.assertEqual(C.judge(root, self.manifest)['rc'], 0)
            result = S.run([sys.executable, '-I', '-B', str(LIB / 'upgrade_tx_fixture.py'), 'judge',
                            '--manifest', str(root / 'manifest.json'), '--evidence', str(root)], 10)
            self.assertEqual(result['native_rc'], 0, result)
            self.assertEqual(before, {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in root.rglob('*') if path.is_file()})
            self.row['native_rc'] = 75
            latest_path = sorted((root / 'samples').glob('*.json'))[-1]
            latest = C.read(latest_path)
            latest['native_rc'] = 75
            S.write_json(latest_path, latest)
            self.rejected(lambda: C.judge(root, self.manifest), 'native_result_mismatch', 3)

    def test_late_terminal_is_not_erased_by_second_drain(self):
        with tempfile.TemporaryDirectory(prefix='s8-drain-') as temporary:
            root = Path(temporary)
            evidence(root, self.row, self.originals, self.manifest)
            rows = drains(self.originals)
            rows[1]['terminals'][0]['result_digest'] = OTHER
            S.write_json(root / 'drains.json', rows)
            self.rejected(lambda: C.judge(root, self.manifest), 'late_terminal_or_history_changed', 3)

    def test_two_drains_have_fresh_nonce_and_a_quiet_interval(self):
        with tempfile.TemporaryDirectory(prefix='s8-quiet-') as temporary:
            root = Path(temporary)
            evidence(root, self.row, self.originals, self.manifest)
            for defect in ('nonce', 'quiet'):
                rows = drains(self.originals)
                if defect == 'nonce': rows[1]['nonce'] = rows[0]['nonce']
                else: rows[1]['at_ns'] = rows[0]['at_ns'] + 1
                S.write_json(root / 'drains.json', rows)
                self.rejected(lambda: C.judge(root, self.manifest), 'drain_not_fresh_or_quiet', 1)

    def test_unknown_custody_requires_death_and_no_reverse_proof(self):
        self.row.update(phase='recovery_required', outcome='unknown', native_rc=74, known=False, complete=False, admission_complete=False)
        for resource in self.row['resources']: resource['known'] = False
        for field in ('control_receipt', 'resources_receipt', 'released_receipt'):
            self.row[field].update(known=False, complete=False)
        proof = self.row['safety_receipt']
        proof.update(known=True, complete=True, original_owner_dead=True, current_owner_known=False)
        C.validate_sample(self.row, self.originals, NONCE, allow_unknown=True)
        self.rejected(lambda: C.validate_sample(self.row, self.originals, NONCE), 'observation_unknown', 2)
        for defect in ('reverse', 'unknown', 'stale'):
            saved = copy.deepcopy(proof)
            if defect == 'reverse': proof['reverse_execs'] = 1
            elif defect == 'unknown': proof['known'] = False
            else: proof['cut_id'] = 'f'*32
            self.rejected(lambda: C.validate_sample(self.row, self.originals, NONCE, allow_unknown=True),
                          'unknown_owner_safety_', 1)
            self.row['safety_receipt'] = saved
            proof = saved
        row = drains(self.originals)[0]
        row.update(joined=False, writers_retired=False, pending_gc=['unsettled.permit'])
        C.validate_drain(row, self.originals, allow_unknown=True)
        self.rejected(lambda: C.validate_drain(row, self.originals), 'drain_ack_or_join_missing', 2)
        self.manifest.update(arm='W08-F0-single-D-unknown-none', case_id='W08-F0-single-D-unknown-none')
        self.row['arm'] = self.manifest['arm']
        self.manifest['expect'].update(phase='recovery_required', outcome='unknown', native_rc=74, completion=False, a6=False, hit_required=True)
        self.originals['manifest_digest'] = C.digest(self.manifest)
        self.originals['original_set_digest'] = C.digest({key:value for key,value in self.originals.items() if key != 'original_set_digest'})
        self.row['original_set_digest'] = self.originals['original_set_digest']
        with tempfile.TemporaryDirectory(prefix='s8-unknown-') as temporary:
            evidence(temporary, self.row, self.originals, self.manifest)
            self.assertTrue(C.judge(temporary, self.manifest)['non_hot'])

    def test_missing_s7_port_and_short_transfer_never_reduce_profile(self):
        with tempfile.TemporaryDirectory(prefix='s8-manifest-') as temporary:
            path = Path(temporary) / 'manifest.json'
            C.publish(path, self.manifest)
            F.manifest_read(path)
            self.manifest['actors'].pop()
            S.write_json(path, self.manifest)
            self.rejected(lambda: F.manifest_read(path), 'full_profile_actor_missing', 2)
            self.manifest = synthetic_manifest(self.row)
            del self.manifest['ports']['admissions']
            S.write_json(path, self.manifest)
            self.rejected(lambda: F.manifest_read(path), 'required_observer_port_missing', 2)
            self.manifest = synthetic_manifest(self.row)
            self.manifest['transfer_budget']['size_bytes'] = 1
            S.write_json(path, self.manifest)
            self.rejected(lambda: F.manifest_read(path), 'original_transfer_will_expire', 2)

    def test_refusing_dependency_cleans_all_started_actors(self):
        with tempfile.TemporaryDirectory(prefix='s8-start-') as temporary:
            root = Path(temporary) / 'e'
            reader, writer = os.pipe()
            try:
                self.rejected(lambda: F.start(root, self.manifest, reader), 'observer_native_failure', 2)
                self.assertTrue(S.wait_empty(root)['empty'])
                self.assertFalse((root / 'originals.json').exists())
            finally:
                os.close(reader)
                os.close(writer)
