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

class UpgradeScenarioOracle(unittest.TestCase):
    # origin: hot-update S8 plan §8.2 / §9.0 / P26b. Transaction outcome is
    # independent of custody: errno and revocation can leave a known owner.
    def test_unknown_outcome_does_not_declare_unknown_ownership(self):
        _, _, manifest = sealed_pair()
        manifest['expect'].update(phase='recovery_required', outcome='unknown', completion=False, a6=False)
        for arm in ('W03-F3-N3-L-known-none', 'W04-F4-agent-L-known-none',
                    'W09-F6-agent-D-known-none'):
            manifest['arm'] = arm
            self.assertFalse(C.unknown_lane(manifest), arm)
        manifest['arm'] = 'W08-F0-single-D-unknown-none'
        self.assertTrue(C.unknown_lane(manifest))
        manifest['arm'] = 'C-owner-dead'
        manifest['expect']['phase'] = 'abandoned'
        self.assertTrue(C.unknown_lane(manifest))

    # origin: hot-update S8 plan §9.0 / P26b. The drill must consume the
    # actual sole judge result, not a hand-built imitation of that result.
    def test_judge_emits_closed_scenario_consumed_by_drill(self):
        row, originals, manifest = sealed_pair()
        with tempfile.TemporaryDirectory(prefix='s8-scenario-') as temporary:
            evidence(temporary, row, originals, manifest)
            result = C.judge(temporary, manifest)
            self.assertEqual(set(result['scenario']), {'version', 'run_id', 'arm', 'source_commit',
                                                     'original_set_digest', 'proofs', 'rc'})
            D = import_drill()
            contract = C.arm_contract(manifest)
            self.assertEqual(D['checked_judgment'](result, contract, manifest, originals), 0)
            C.schema_definition(result, 'judgment')
            for key in ('scenario',):
                changed = copy.deepcopy(result)
                changed[key]['private_state'] = 'forbidden'
                with self.assertRaisesRegex(C.EvidenceError, 'schema_unknown_'): C.schema_definition(changed, 'judgment')
            for key in result['scenario']:
                changed = copy.deepcopy(result)
                del changed['scenario'][key]
                with self.assertRaisesRegex(C.EvidenceError, 'schema_missing_'): C.schema_definition(changed, 'judgment')

    # origin: hot-update S8 plan §8.3 / P26b. A passing native verifier of
    # a negative control preserves the product mutant's independently red rc.
    def test_scenario_preserves_native_mutant_red(self):
        row, originals, manifest = arm_pair('M19')
        with tempfile.TemporaryDirectory(prefix='s8-mutant-oracle-') as temporary:
            root = Path(temporary)
            evidence(root, row, originals, manifest)
            # Pure reader input plus the real Python source builder. The
            # execution/verifier records below are oracle controls, never
            # emitted as product mutation receipts by this hermetic test.
            contract = C.arm_contract(manifest)
            recipe = I.mutation_recipe(contract)
            archive = root / 'source.tar'
            with tarfile.open(archive, 'w') as stream:
                for name, raw in [('go.mod', b'module example.invalid/control\n'),
                                  (recipe['file'], (LIB / 'upgrade_tx_collect.py').read_bytes())]:
                    info = tarfile.TarInfo(name)
                    info.size = len(raw)
                    stream.addfile(info, io.BytesIO(raw))
            artifact = dict(source_archive=str(archive), archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
                            binary_sha256=SHA, commit=manifest['source_commit'])
            build = I.build_mutation(root/'mutation', contract, artifact, '/unused/go', '/unused/supervisor')
            C.publish(root/'runner-envelope.json', dict(mutation=str(root/'mutation/mutation-build.json'),
                                                       mutation_patch_digest=build['patch_digest']))
            execution = dict(version=1,arm='M19',run_id=manifest['run_id'],owner={k:v for k,v in OWNER.items() if k!='instance_id'},
                             source_digest=build['source_digest'],binary_sha=build['binary_sha'],patch_digest=build['patch_digest'],
                             executed_path=str(root/'mutation/source'/recipe['file']),executable_sha=SHA,argv=['/usr/bin/python3','-I','-B',str(root/'mutation/source'/recipe['file'])],
                             begin_ns=1,end_ns=2)
            C.publish(root/'mutation-execution.json', execution)
            companion = C.read(root/'scenario-companion.json')
            payload = C.read(root/'companion-payload.json')
            payload.update(Mutant='M19',NativeRC=1,ExecutedPath=execution['executed_path'],BinarySHA=build['binary_sha'],SourceDigest=build['source_digest'])
            payload['Seal']['BinarySHA'] = SHA
            del payload['RunID']  # The actual P25 mutant DTO carries RunID only in Seal.
            companion['files'] = [file_reference(root,'companion-payload.json',payload)]
            companion['rc'] = 1
            validation = C.read(root/'companion-validation.json')
            validation['files'] = companion['files']
            validation['events'] = [dict(Action='run',Test=contract['test'])]
            for number in range(5):
                name=contract['test']+'/M19'+('' if number==0 else '#%02d'%number)
                validation['events'] += [dict(Action=action,Test=name) for action in ('run','pass')]
            validation['events'].append(dict(Action='pass',Test=contract['test']))
            validation['process']['stdout']='\n'.join(json.dumps(event) for event in validation['events'])
            invocation=C.read(root/'companion-invocation.json')
            invocation.update(corpus=companion['files'],process=validation['process'])
            validation['invocation']=file_reference(root,'companion-invocation.json',invocation)
            companion['validation'] = file_reference(root,'companion-validation.json',validation)
            S.write_json(root/'scenario-companion.json',companion)
            result = C.judge(root, manifest)
            self.assertEqual((result['rc'], result['scenario']['rc'], result['observation_rc']), (1, 1, 0))
            self.assertEqual(import_drill()['checked_judgment'](result,C.arm_contract(manifest),manifest,originals),1)
            for defect in ('wrong_source','wrong_execution','unrelated_oracle','wrong_subject'):
                bad_payload, bad_execution, bad_validation = copy.deepcopy(payload), copy.deepcopy(execution), copy.deepcopy(validation)
                if defect=='wrong_source': bad_payload['SourceDigest']='f'*64
                elif defect=='wrong_execution': bad_execution['binary_sha']='f'*64
                elif defect=='wrong_subject': bad_execution['argv']=['/usr/bin/python3','/unmodified/script.py']
                else:
                    bad_validation['events']=[dict(Action=action,Test=contract['test']) for action in ('run','pass')]
                    bad_validation['process']['stdout']='\n'.join(json.dumps(event) for event in bad_validation['events'])
                companion['files']=[file_reference(root,'companion-payload.json',bad_payload)]
                bad_validation['files']=companion['files']
                bad_invocation=copy.deepcopy(invocation)
                bad_invocation.update(corpus=companion['files'],process=bad_validation['process'])
                bad_validation['invocation']=file_reference(root,'companion-invocation.json',bad_invocation)
                companion['validation']=file_reference(root,'companion-validation.json',bad_validation)
                S.write_json(root/'scenario-companion.json',companion)
                S.write_json(root/'mutation-execution.json',bad_execution)
                expected={'wrong_source':'scenario_mutation_build_foreign','wrong_execution':'scenario_mutation_not_running',
                          'unrelated_oracle':'scenario_mutation_oracle_not_reached','wrong_subject':'mutation_script_not_executed'}[defect]
                with self.assertRaisesRegex(C.EvidenceError,expected): C.judge(root,manifest)
            S.write_json(root/'mutation-execution.json',execution)
            validation['invocation']=file_reference(root,'companion-invocation.json',invocation)
            companion['validation']=file_reference(root,'companion-validation.json',validation)
            for defect in ('green', 'wrong_mutant', 'same_binary'):
                changed = copy.deepcopy(payload)
                if defect == 'green': changed['NativeRC'] = 0
                elif defect == 'wrong_mutant': changed['Mutant'] = 'M02'
                else: changed['BinarySHA'] = SHA
                companion['files'] = [file_reference(root,'companion-payload.json',changed)]
                S.write_json(root/'scenario-companion.json',companion)
                with self.assertRaisesRegex(C.EvidenceError,'scenario_mutant_not_executed'): C.judge(root,manifest)

    # origin: hot-update S8 plan §9.0 / P26b. A valid P23/P25 handback must
    # include the actual inputs AND an independent native verifier receipt.
    def test_scenario_requires_bound_companion_files_and_native_validation(self):
        row, originals, manifest = arm_pair('C-driver')
        with tempfile.TemporaryDirectory(prefix='s8-companion-') as temporary:
            root = Path(temporary)
            evidence(root, row, originals, manifest)
            result = C.judge(root, manifest)
            self.assertEqual(import_drill()['checked_judgment'](result, C.arm_contract(manifest), manifest, originals), 0)
            baseline = {p:p.read_bytes() for p in root.glob('*.json')}
            companion_path = root / 'scenario-companion.json'
            for defect in ('missing', 'foreign_run', 'foreign_arm', 'foreign_source', 'foreign_artifact', 'foreign_original',
                           'proof_missing', 'proof_duplicate', 'test_foreign', 'payload_missing', 'payload_digest',
                           'payload_foreign', 'payload_arm', 'payload_roster', 'payload_owner', 'seal_missing', 'seal_source', 'seal_run', 'seal_binary', 'validation_missing',
                           'validation_foreign', 'validation_input', 'validation_failed', 'validation_skip', 'validation_no_run',
                           'validation_no_pass', 'validation_interval', 'validation_proofs', 'validation_process', 'validation_stdout',
                           'validation_package_fail', 'validation_elapsed', 'file_reused', 'path_escape',
                           'unknown_field', 'rc_laundered'):
                with self.subTest(defect=defect):
                    for path, raw in baseline.items(): path.write_bytes(raw)
                    companion = C.read(companion_path)
                    if defect == 'missing': companion_path.unlink()
                    elif defect.startswith('foreign_'):
                        key = dict(run='run_id', arm='arm', source='source_commit', artifact='artifact_sha', original='original_set_digest')[defect[8:]]
                        companion[key] = ('f' * len(companion[key])) if key != 'arm' else 'C-reopen'
                    elif defect == 'proof_missing': companion['proofs'].pop()
                    elif defect == 'proof_duplicate': companion['proofs'].append(companion['proofs'][0])
                    elif defect == 'test_foreign': companion['test'] = 'TestActionCrashRecovery'
                    elif defect == 'file_reused': companion['files'].append(companion['files'][0])
                    elif defect == 'path_escape': companion['files'][0]['path'] = '../foreign.json'
                    elif defect == 'unknown_field': companion['secret'] = 'forbidden'
                    elif defect == 'rc_laundered': companion['rc'] = 3
                    elif defect.startswith(('payload_', 'seal_')):
                        path = root / companion['files'][0]['path']
                        payload = C.read(path)
                        if defect == 'payload_missing': path.unlink()
                        elif defect == 'payload_digest': path.write_text('{}')
                        else:
                            if defect == 'payload_foreign': payload['RunID'] = 'other-run'
                            elif defect == 'payload_arm': payload['Arm'] = 'C-reopen'
                            elif defect == 'payload_roster': payload['Seal']['Original']['expected'].pop()
                            elif defect == 'payload_owner': payload['Seal']['Original']['expected'][0]['owner']['start_ticks'] += 1
                            elif defect == 'seal_source': payload['Seal']['SourceCommit'] = 'f'*40
                            elif defect == 'seal_run': payload['Seal']['RunID'] = 'other-run'
                            elif defect == 'seal_binary': payload['Seal']['BinarySHA'] = 'f'*64
                            else: del payload['Seal']
                            companion['files'][0] = file_reference(root, path.name, payload)
                    else:
                        path = root / companion['validation']['path']
                        validation = C.read(path)
                        if defect == 'validation_missing': path.unlink()
                        else:
                            if defect == 'validation_foreign': validation['run_id'] = 'f'*32
                            elif defect == 'validation_input': validation['files'][0]['sha256'] = 'f'*64
                            elif defect == 'validation_failed': validation['native_rc'] = 1
                            elif defect == 'validation_skip': validation['events'].insert(1, dict(Action='skip', Test=validation['test']+'/child'))
                            elif defect == 'validation_no_run': validation['events'].pop(0)
                            elif defect == 'validation_no_pass': validation['events'].pop()
                            elif defect == 'validation_interval': validation['end_ns'] = validation['begin_ns']
                            elif defect == 'validation_proofs': validation['proofs'].pop()
                            elif defect == 'validation_process': validation['process']['owner']['pid'] = 0
                            elif defect == 'validation_stdout': validation['process']['stdout'] = json.dumps(dict(Action='run',Test=validation['test']))
                            elif defect == 'validation_package_fail': validation['process']['stdout'] += '\n'+json.dumps(dict(Action='fail',Package='hotupdate_test'))
                            elif defect == 'validation_elapsed': validation['process']['stdout'] = json.dumps(dict(Action='pass',Test=validation['test'],Elapsed=True))
                            if defect in ('validation_skip','validation_no_run','validation_no_pass'):
                                validation['process']['stdout'] = '\n'.join(json.dumps(event) for event in validation['events'])
                            companion['validation'] = file_reference(root, path.name, validation)
                    if defect != 'missing': S.write_json(companion_path, companion)
                    with self.assertRaises(C.EvidenceError) as caught: C.judge(root, manifest)
                    self.assertIn(caught.exception.rc, (2, 4))
                    expected_reason = {
                        'missing':'evidence_missing_or_oversize', 'proof_missing':'scenario_companion_proof_missing',
                        'proof_duplicate':'scenario_companion_proof_missing', 'test_foreign':'scenario_companion_test_foreign',
                        'payload_missing':'scenario_evidence_missing', 'payload_digest':'scenario_evidence_digest_changed',
                        'payload_foreign':'scenario_payload_foreign', 'payload_roster':'scenario_original_companion_roster_changed',
                        'payload_arm':'scenario_payload_foreign', 'seal_source':'scenario_original_companion_foreign',
                        'seal_run':'scenario_original_companion_foreign', 'seal_binary':'scenario_original_companion_foreign',
                        'payload_owner':'scenario_original_companion_owner_changed', 'seal_missing':'scenario_original_companion_missing',
                        'validation_missing':'scenario_evidence_missing', 'validation_foreign':'scenario_validation_foreign',
                        'validation_input':'scenario_validation_foreign', 'validation_failed':'scenario_validation_not_passed',
                        'validation_skip':'scenario_validation_native_failure', 'validation_no_run':'scenario_validation_test_not_passed',
                        'validation_no_pass':'scenario_validation_test_not_passed', 'validation_interval':'scenario_validation_interval_invalid',
                        'validation_proofs':'scenario_validation_foreign', 'file_reused':'scenario_companion_file_reused',
                        'validation_process':'scenario_validation_process_invalid', 'validation_stdout':'scenario_validation_stdout_changed',
                        'validation_package_fail':'scenario_validation_native_failure', 'validation_elapsed':'schema_type_go_test_event.Elapsed',
                        'path_escape':'scenario_evidence_path_foreign', 'unknown_field':'schema_unknown_scenario_companion',
                        'rc_laundered':'scenario_native_rc_not_observed',
                    }.get(defect, 'scenario_companion_foreign')
                    self.assertEqual(str(caught.exception), expected_reason)

    # origin: hot-update S8 plan §9.0 / F8. Bind the native verifier to its
    # captured input and source; a legitimate PASS from another argv is stale.
    def test_companion_native_invocation_cannot_be_relabelled(self):
        row, originals, manifest = arm_pair('C-driver')
        with tempfile.TemporaryDirectory(prefix='s8-native-binding-') as temporary:
            root=Path(temporary)
            evidence(root,row,originals,manifest)
            companion=C.read(root/'scenario-companion.json')
            validation=C.read(root/'companion-validation.json')
            invocation=C.read(root/'companion-invocation.json')
            for defect in ('schema','command','directory','input','process','source','time'):
                changed=copy.deepcopy(invocation)
                if defect=='schema': changed['claimed_pass']=True
                elif defect=='command': changed['command'][3]='version'
                elif defect=='directory': changed['cwd']='/different/source'
                elif defect=='input': changed['corpus']=[]
                elif defect=='process': changed['process']['native_rc']=1
                elif defect=='source': changed['sources']={}
                else: changed['begin_ns']+=1
                validation['invocation']=file_reference(root,'companion-invocation.json',changed)
                companion['validation']=file_reference(root,'companion-validation.json',validation)
                S.write_json(root/'scenario-companion.json',companion)
                expected=('scenario_invocation_schema_invalid' if defect=='schema' else
                          'scenario_verifier_command_unbound' if defect in {'command','directory'} else
                          'scenario_verifier_input_unbound' if defect=='input' else 'scenario_verifier_receipt_unbound')
                with self.subTest(defect=defect),self.assertRaisesRegex(C.EvidenceError,expected): C.judge(root,manifest)

    # origin: hot-update S8 plan §9.0 / P26b. The declaration cannot remove
    # a required oracle or use a scenario that was never in the frozen table.
    def test_scenario_declaration_cannot_remove_required_evidence(self):
        with tempfile.TemporaryDirectory(prefix='s8-n3-manifest-') as temporary:
            _, _, manifest = arm_pair('A-n3')
            path = Path(temporary)/'manifest.json'
            S.write_json(path,manifest)
            self.assertEqual(F.manifest_read(path)['role'],'N3')
        with tempfile.TemporaryDirectory(prefix='s8-declaration-') as temporary:
            row, originals, manifest = sealed_pair()
            evidence(temporary,row,originals,manifest)
            for defect, reason in [('arm','scenario_arm_not_declared'), ('expect','scenario_expectation_changed')]:
                changed = copy.deepcopy(manifest)
                if defect == 'arm': changed['arm'] = 'undeclared'
                else: changed['expect']['native_rc'] = 75
                with self.assertRaisesRegex(C.EvidenceError,reason): C.judge(temporary,changed)
        with tempfile.TemporaryDirectory(prefix='s8-missing-boundary-') as temporary:
            row, originals, manifest = arm_pair('H-root')
            evidence(temporary,row,originals,manifest)
            with self.assertRaisesRegex(C.EvidenceError,'scenario_cold_boundary_missing'): C.judge(temporary,manifest)
        with tempfile.TemporaryDirectory(prefix='s8-no-calibration-') as temporary:
            row, originals, manifest = sealed_pair()
            manifest['expect']['a6'] = False
            originals = reseal(row,manifest)
            evidence(temporary,row,originals,manifest)
            with self.assertRaisesRegex(C.EvidenceError,'scenario_a6_oracle_disabled'): C.judge(temporary,manifest)

    # origin: hot-update S8 plan §8.2 / P26b. Known errno/revocation owners
    # retain the full observation and admission requirements, even at rc74/77.
    def test_known_owner_errno_and_revocation_keep_strict_census(self):
        for arm in ('W04-F4-agent-L-known-none', 'W09-F6-agent-D-known-none'):
            row, originals, manifest = arm_pair(arm)
            C.validate_sample(row, originals, NONCE, allow_unknown=C.unknown_lane(manifest))
            row['known'] = False
            with self.assertRaisesRegex(C.EvidenceError, 'observation_unknown'):
                C.validate_sample(row, originals, NONCE, allow_unknown=C.unknown_lane(manifest))

    # origin: hot-update S8 plan §8.1 / P26b. Archive consumes independent
    # death and empty-custody receipts; it cannot create missing business results.
    def test_owner_death_archive_requires_independent_empty_custody(self):
        row, originals, manifest = arm_pair('C-owner-dead')
        row.update(known=False, complete=False, admission_complete=False)
        for resource in row['resources']: resource['known'] = False
        for field in ('control_receipt', 'resources_receipt', 'released_receipt'): row[field].update(known=False, complete=False)
        row['safety_receipt'].update(known=True, complete=True, original_owner_dead=True, current_owner_known=False)
        archive = {key:row[key] for key in ('nonce','tx_id','cut_id','generation','scope_digest')}
        archive.update(known=True, complete=True, owner=copy.deepcopy(OWNER), evidence_digest=OTHER,
                       at_ns=row['observed_end_ns'], current_released=True, remaining=[], phase='abandoned', outcome='unknown',
                       resource_ids=[r['id'] for r in row['resources']])
        row['archive_receipt'] = archive
        C.validate_sample(row, originals, NONCE, allow_unknown=C.unknown_lane(manifest))
        for defect, reason in [('missing','archive_custody_evidence_missing'), ('unknown','archive_custody_not_empty'),
                               ('retained','archive_custody_not_empty'), ('unreleased','archive_custody_not_empty'),
                               ('reused','archive_custody_stale'), ('owner','archive_custody_stale'),
                               ('clock','archive_custody_stale'), ('roster','archive_custody_roster_missing')]:
            with self.subTest(defect=defect):
                changed = copy.deepcopy(row)
                proof = changed['archive_receipt']
                if defect == 'missing': del changed['archive_receipt']
                elif defect == 'unknown': proof['known'] = False
                elif defect == 'retained': proof['remaining'] = [OWNER]
                elif defect == 'unreleased': proof['current_released'] = False
                elif defect == 'reused': proof['evidence_digest'] = SHA
                elif defect == 'owner': proof['owner']['start_ticks'] += 1
                elif defect == 'clock': proof['at_ns'] += 1
                else: proof['resource_ids'].pop()
                with self.assertRaisesRegex(C.EvidenceError, reason): C.validate_sample(changed, originals, NONCE, allow_unknown=True)
        with tempfile.TemporaryDirectory(prefix='s8-archive-') as temporary:
            evidence(temporary, row, originals, manifest)
            quiet = drains(originals)
            for drain in quiet:
                for terminal in drain['terminals']: terminal['terminal_count'] = 0
                for history in drain['history']: history.update(canonical_terminal=0, physical_records=1)
            S.write_json(Path(temporary)/'drains.json', quiet)
            result = C.judge(temporary, manifest)
            self.assertTrue(result['non_hot'])
            self.assertEqual(import_drill()['checked_judgment'](result, C.arm_contract(manifest), manifest, originals), 0)
            for defect in ('pending_gc', 'joined'):
                changed = copy.deepcopy(quiet)
                if defect == 'pending_gc': changed[0]['pending_gc'] = ['retained']
                else: changed[0]['joined'] = False
                S.write_json(Path(temporary)/'drains.json', changed)
                with self.assertRaises(C.EvidenceError): C.judge(temporary, manifest)

    # origin: hot-update S8 plan §5.2 / P26b. Only an independently drained
    # reverse-to-cold boundary exempts terminated originals from live receipts.
    def test_cold_boot_boundary_is_explicit_and_cannot_reconnect_originals(self):
        with tempfile.TemporaryDirectory(prefix='s8-cold-') as temporary:
            root = Path(temporary)
            row, originals, manifest = cold_evidence(root)
            self.assertTrue(C.judge(root, manifest)['non_hot'])
            saved = {p:p.read_bytes() for p in root.rglob('*.json')}
            for defect, reason in [('missing','resource_identity_unknown'), ('foreign','cold_boundary_foreign'),
                                   ('wrong_arm','cold_boundary_not_declared'), ('interval','cold_boundary_interval_invalid'),
                                   ('overlap','cold_boundary_observation_overlaps'), ('reverse','cold_boundary_reverse_missing'),
                                   ('selector','cold_boundary_owner_or_selector_changed'), ('same_owner','cold_boundary_owner_or_selector_changed'),
                                   ('same_process','cold_boundary_owner_or_selector_changed'),
                                   ('drain','cold_boundary_drains_missing'), ('unsettled','cold_boundary_cohort_not_settled'),
                                   ('roster','cold_boundary_original_missing'), ('recreate','cold_boundary_original_recreated'),
                                   ('reconnect','original_identity_or_reconnect_changed'), ('hot_loss','resource_identity_unknown')]:
                with self.subTest(defect=defect):
                    for path, raw in saved.items(): path.write_bytes(raw)
                    boundary = C.read(root/'cold-boot.json')
                    changed = copy.deepcopy(row)
                    changed_manifest = copy.deepcopy(manifest)
                    if defect == 'missing': (root/'cold-boot.json').unlink()
                    elif defect == 'foreign': boundary['run_id'] = 'f'*32
                    elif defect == 'wrong_arm': changed_manifest['arm'] = 'R2'
                    elif defect == 'interval': boundary['end_ns'] = boundary['begin_ns']
                    elif defect == 'overlap': boundary['end_ns'] = changed['observed_end_ns']
                    elif defect == 'reverse':
                        before = C.read(root/'samples'/('4'*32+'.json')); before['direction'] = 'forward'
                        S.write_json(root/'samples'/('4'*32+'.json'), before)
                    elif defect == 'selector': boundary['selected_sha'] = OTHER
                    elif defect == 'same_owner': boundary['owner_after'] = boundary['owner_before']
                    elif defect == 'same_process':
                        boundary['owner_after'] = dict(boundary['owner_before'],instance_id='new-runtime-same-process')
                        changed.update(pid=OWNER['pid'],start_ticks=OWNER['start_ticks'],owner_boot_id=OWNER['boot_id'])
                    elif defect == 'drain': boundary['drains'].append(boundary['drains'][0])
                    elif defect == 'unsettled': boundary['begin_ns'] = 1_002_000_000_000
                    elif defect == 'roster': boundary['retired_ids'].pop()
                    elif defect == 'recreate': changed['resources'][0]['known'] = True
                    elif defect == 'reconnect': changed['resources'][0]['reconnects'] += 1
                    else:
                        changed.update(observed_begin_ns=999_000_000_000, observed_end_ns=999_000_000_100)
                    if defect != 'missing': S.write_json(root/'cold-boot.json', boundary)
                    with self.assertRaisesRegex(C.EvidenceError, reason):
                        retired = C.cold_boundary(root, changed_manifest, changed, originals)
                        C.validate_sample(changed, originals, changed['nonce'], retired=retired)
            for path, raw in saved.items(): path.write_bytes(raw)
            before = C.read(root/'samples'/('4'*32+'.json'))
            before['resources'][0]['reconnects'] += 1
            S.write_json(root/'samples'/('4'*32+'.json'), before)
            with self.assertRaisesRegex(C.EvidenceError, 'original_identity_or_reconnect_changed'): C.judge(root, manifest)
            for path, raw in saved.items(): path.write_bytes(raw)
            latest = C.read(root/'samples'/(row['nonce']+'.json'))
            latest['resources'] = [r for r in latest['resources'] if r['id'] != 'post-cold.control']
            latest['admissions'] = []
            for field in ('control_receipt','resources_receipt','released_receipt'): latest[field]['census']['resources'] = []
            admissions = C.read(root/'admissions'/(row['nonce']+'.json'))
            admissions['resources'] = []
            S.write_json(root/'samples'/(row['nonce']+'.json'),latest)
            S.write_json(root/'admissions'/(row['nonce']+'.json'),admissions)
            with self.assertRaisesRegex(C.EvidenceError,'receipt_live_census_empty'): C.judge(root,manifest)
