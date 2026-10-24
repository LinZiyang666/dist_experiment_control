#!/bin/sh
# origin: hot-update S8 plan §9.2 / §11.2 P27. One arm per runner unit.
# arms: A-single A-sole A-n3 R0 R1 R2 H-root H-user H-no-systemd H-colocated H-sibling H-readonly G-two G-third G-concurrent C-driver C-reopen C-gc C-late-writer C-history-rollback C-owner-dead W00-F0-agent-L-known-none W01-F1-single-L-known-none W02-F2-sole-voter-L-known-none W03-F3-N3-L-known-none W04-F4-agent-L-known-none W05-F5-single-L-known-inside W06-F6-sole-voter-L-known-none W07-F7-N3-L-known-inside W08-F0-agent-D-known-none W09-F1-single-D-known-none W10-F2-sole-voter-D-known-none W11-F8-N3-D-known-none W12-F9-N3-D-known-none W05-F5-single-L-known-outside W07-F7-N3-L-known-outside W09-F5-agent-D-known-inside W09-F5-agent-D-known-outside W09-F7-N3-D-known-inside W09-F7-N3-D-known-outside W08-F0-single-D-unknown-none W09-F1-agent-D-unknown-none W12-F9-N3-D-unknown-none W08-F3-single-D-known-none W09-F4-sole-voter-D-known-none W09-F6-agent-D-known-none I-late-revoke-current-rollback I-nats-cut-leader-move I-begin-archive-gc I-driver-death-host-reservation M01 M02 M03 M04 M05 M06 M07 M08 M09 M10 M11 M12 M13 M14 M15 M16 M17 M18 M19 M20 M21
# fixture: A-single=N1 A-sole=N1 A-n3=N3-live R0=N1 R1=N1 R2=N1 H-root=N1 H-user=N1 H-no-systemd=N1 H-colocated=N1 H-sibling=N1 H-readonly=N1 G-two=N1 G-third=N1 G-concurrent=N1 C-driver=N1 C-reopen=N1 C-gc=N1 C-late-writer=N1 C-history-rollback=N1 C-owner-dead=N1 W00-F0-agent-L-known-none=N1 W01-F1-single-L-known-none=N1 W02-F2-sole-voter-L-known-none=N1 W03-F3-N3-L-known-none=N3-live W04-F4-agent-L-known-none=N1 W05-F5-single-L-known-inside=N1 W06-F6-sole-voter-L-known-none=N1 W07-F7-N3-L-known-inside=N3-live W08-F0-agent-D-known-none=N1 W09-F1-single-D-known-none=N1 W10-F2-sole-voter-D-known-none=N1 W11-F8-N3-D-known-none=N3-live W12-F9-N3-D-known-none=N3-live W05-F5-single-L-known-outside=N1 W07-F7-N3-L-known-outside=N3-live W09-F5-agent-D-known-inside=N1 W09-F5-agent-D-known-outside=N1 W09-F7-N3-D-known-inside=N3-live W09-F7-N3-D-known-outside=N3-live W08-F0-single-D-unknown-none=N1 W09-F1-agent-D-unknown-none=N1 W12-F9-N3-D-unknown-none=N3-live W08-F3-single-D-known-none=N1 W09-F4-sole-voter-D-known-none=N1 W09-F6-agent-D-known-none=N1 I-late-revoke-current-rollback=N1 I-nats-cut-leader-move=N3-live I-begin-archive-gc=N1 I-driver-death-host-reservation=N3-live M01=N1 M02=N1 M03=N1 M04=N1 M05=N1 M06=N1 M07=N1 M08=N1 M09=N1 M10=N1 M11=N1 M12=N1 M13=N1 M14=N1 M15=N1 M16=N1 M17=N1 M18=N1 M19=N1 M20=N1 M21=N1
# grows: A-single=0 A-sole=0 A-n3=2 R0=0 R1=0 R2=0 H-root=0 H-user=0 H-no-systemd=0 H-colocated=0 H-sibling=0 H-readonly=0 G-two=0 G-third=0 G-concurrent=0 C-driver=0 C-reopen=0 C-gc=0 C-late-writer=0 C-history-rollback=0 C-owner-dead=0 W00-F0-agent-L-known-none=0 W01-F1-single-L-known-none=0 W02-F2-sole-voter-L-known-none=0 W03-F3-N3-L-known-none=2 W04-F4-agent-L-known-none=0 W05-F5-single-L-known-inside=0 W06-F6-sole-voter-L-known-none=0 W07-F7-N3-L-known-inside=2 W08-F0-agent-D-known-none=0 W09-F1-single-D-known-none=0 W10-F2-sole-voter-D-known-none=0 W11-F8-N3-D-known-none=2 W12-F9-N3-D-known-none=2 W05-F5-single-L-known-outside=0 W07-F7-N3-L-known-outside=2 W09-F5-agent-D-known-inside=0 W09-F5-agent-D-known-outside=0 W09-F7-N3-D-known-inside=2 W09-F7-N3-D-known-outside=2 W08-F0-single-D-unknown-none=0 W09-F1-agent-D-unknown-none=0 W12-F9-N3-D-unknown-none=2 W08-F3-single-D-known-none=0 W09-F4-sole-voter-D-known-none=0 W09-F6-agent-D-known-none=0 I-late-revoke-current-rollback=0 I-nats-cut-leader-move=2 I-begin-archive-gc=0 I-driver-death-host-reservation=2 M01=0 M02=0 M03=0 M04=0 M05=0 M06=0 M07=0 M08=0 M09=0 M10=0 M11=0 M12=0 M13=0 M14=0 M15=0 M16=0 M17=0 M18=0 M19=0 M20=0 M21=0
# worst: A-single=1100 A-sole=1100 A-n3=1100 R0=120 R1=120 R2=120 H-root=120 H-user=120 H-no-systemd=120 H-colocated=120 H-sibling=120 H-readonly=120 G-two=120 G-third=120 G-concurrent=120 C-driver=120 C-reopen=120 C-gc=120 C-late-writer=120 C-history-rollback=120 C-owner-dead=120 W00-F0-agent-L-known-none=180 W01-F1-single-L-known-none=180 W02-F2-sole-voter-L-known-none=180 W03-F3-N3-L-known-none=180 W04-F4-agent-L-known-none=180 W05-F5-single-L-known-inside=180 W06-F6-sole-voter-L-known-none=180 W07-F7-N3-L-known-inside=180 W08-F0-agent-D-known-none=180 W09-F1-single-D-known-none=180 W10-F2-sole-voter-D-known-none=180 W11-F8-N3-D-known-none=180 W12-F9-N3-D-known-none=180 W05-F5-single-L-known-outside=180 W07-F7-N3-L-known-outside=180 W09-F5-agent-D-known-inside=180 W09-F5-agent-D-known-outside=180 W09-F7-N3-D-known-inside=180 W09-F7-N3-D-known-outside=180 W08-F0-single-D-unknown-none=180 W09-F1-agent-D-unknown-none=180 W12-F9-N3-D-unknown-none=180 W08-F3-single-D-known-none=180 W09-F4-sole-voter-D-known-none=180 W09-F6-agent-D-known-none=180 I-late-revoke-current-rollback=180 I-nats-cut-leader-move=180 I-begin-archive-gc=180 I-driver-death-host-reservation=180 M01=90 M02=90 M03=90 M04=90 M05=90 M06=90 M07=90 M08=90 M09=90 M10=90 M11=90 M12=90 M13=90 M14=90 M15=90 M16=90 M17=90 M18=90 M19=90 M20=90 M21=90
# forgoes: A-single=- A-sole=- A-n3=- R0=- R1=- R2=- H-root=- H-user=- H-no-systemd=- H-colocated=- H-sibling=- H-readonly=- G-two=- G-third=- G-concurrent=- C-driver=- C-reopen=- C-gc=- C-late-writer=- C-history-rollback=- C-owner-dead=- W00-F0-agent-L-known-none=- W01-F1-single-L-known-none=- W02-F2-sole-voter-L-known-none=- W03-F3-N3-L-known-none=- W04-F4-agent-L-known-none=- W05-F5-single-L-known-inside=- W06-F6-sole-voter-L-known-none=- W07-F7-N3-L-known-inside=- W08-F0-agent-D-known-none=- W09-F1-single-D-known-none=- W10-F2-sole-voter-D-known-none=- W11-F8-N3-D-known-none=- W12-F9-N3-D-known-none=- W05-F5-single-L-known-outside=- W07-F7-N3-L-known-outside=- W09-F5-agent-D-known-inside=- W09-F5-agent-D-known-outside=- W09-F7-N3-D-known-inside=- W09-F7-N3-D-known-outside=- W08-F0-single-D-unknown-none=- W09-F1-agent-D-unknown-none=- W12-F9-N3-D-unknown-none=- W08-F3-single-D-known-none=- W09-F4-sole-voter-D-known-none=- W09-F6-agent-D-known-none=- I-late-revoke-current-rollback=- I-nats-cut-leader-move=- I-begin-archive-gc=- I-driver-death-host-reservation=- M01=- M02=- M03=- M04=- M05=- M06=- M07=- M08=- M09=- M10=- M11=- M12=- M13=- M14=- M15=- M16=- M17=- M18=- M19=- M20=- M21=-
# The envelope binds P22's pair, P26's full fixture and product operations.
# P26 judge owns all scenario checks, including P23/P25 companion receipts.
# Missing adapters/proofs are SETUP-RED; they never reduce the profile.
# No private workload or second oracle is built here.
set -u

upgrade38_run() {
    "$SIM_UPGRADE_SUPERVISOR" -- "$S8_PYTHON" -I -B - \
        "$HERE/drills/38-hot-update-rollback-compat.sh" \
        "$HERE/drills/lib/upgrade_tx_arms.tsv" "$SIM_REPO/test/hotupdate/testdata/upgrade_fault_cases.tsv" \
        "$HERE/drills/lib" "$ARM" "$SIM_UPGRADE_MANIFEST" "$SIM_UPGRADE_EVIDENCE" <<'PY38'
import csv
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import secrets
import sys
import time

FIELDS = 'arm family fixture grows worst forgoes role topology case_id point expected_phase expected_outcome cli_rc expected_native kind proofs test operations mutation'.split()
BASE = {'A-single', 'A-sole', 'A-n3', 'R0', 'R1', 'R2',
        *{'H-' + x for x in ('root', 'user', 'no-systemd', 'colocated', 'sibling', 'readonly')},
        *{'G-' + x for x in ('two', 'third', 'concurrent')},
        *{'C-' + x for x in ('driver', 'reopen', 'gc', 'late-writer', 'history-rollback', 'owner-dead')}}
INTERACTIONS = {'I-' + x for x in ('late-revoke-current-rollback', 'nats-cut-leader-move',
                                 'begin-archive-gc', 'driver-death-host-reservation')}
VERDICTS = {0: 'GREEN', 1: 'ASSERT-RED', 2: 'SETUP-RED', 3: 'PRODUCT-RED', 4: 'INCOMPLETE'}


class ContractError(ValueError):
    def __init__(self, reason, rc=2):
        super().__init__(reason)
        self.rc = rc


def need(condition, reason, rc=2):
    if not condition:
        raise ContractError(reason, rc)


def table(path):
    lines = [line for line in Path(path).read_text().splitlines() if not line.startswith('#')]
    reader = csv.DictReader(io.StringIO('\n'.join(lines)), delimiter='\t')
    need(reader.fieldnames == FIELDS, 'arm_table_fields_changed')
    rows = list(reader)
    need(rows and all(set(row) == set(FIELDS) and None not in row.values() for row in rows), 'arm_row_invalid')
    result = {}
    for row in rows:
        arm = row['arm']
        need(re.fullmatch('[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*', arm) and arm not in result, 'arm_duplicate_or_invalid')
        need(row['family'] in 'A R H G C W I M'.split(), 'arm_family_invalid')
        need(row['fixture'] in {'N1', 'N3-live'} and row['forgoes'] == '-', 'arm_fixture_invalid')
        need(row['grows'] == ('2' if row['fixture'] == 'N3-live' else '0'), 'arm_grow_budget_invalid')
        need(60 <= int(row['worst']) <= 14400 and 0 <= int(row['cli_rc']) <= 255
             and int(row['expected_native']) in VERDICTS, 'arm_budget_or_rc_invalid')
        need(row['proofs'] and row['test'] and row['operations'], 'arm_obligation_missing')
        need(row['kind'] in {'hot', 'refusal', 'recovery', 'unknown', 'mutant'}, 'arm_kind_invalid')
        need(row['family'] != 'M' or (row['kind'] == 'mutant' and row['expected_native'] != '0'), 'mutant_cannot_be_green')
        need((row['mutation'] != 'none') == (row['family'] == 'M'), 'mutation_recipe_missing_or_unexpected')
        result[arm] = row
    return result


def contract(script, arms_path, cases_path):
    rows = table(arms_path)
    source = Path(script).read_text()
    # origin: hot-update S8 plan §9.2. Five real headers are consumed by the
    # shared runner. Each flat child must appear in every budget map and dispatch.
    for key in ('arms', 'fixture', 'grows', 'worst', 'forgoes'):
        actual = re.findall(r'^# ' + key + r': (.*)$', source, re.M)
        expected = ' '.join(rows) if key == 'arms' else ' '.join(arm + '=' + row[key] for arm, row in rows.items())
        need(actual == [expected], 'header_table_mismatch_' + key, 1)
    dispatch = source.split('case "${ARM:?}" in\n')
    need(len(dispatch) == 2, 'flat_arm_dispatch_missing', 1)
    labels = re.findall(r'^    ([A-Za-z0-9-]+)\) upgrade38_run ;;$', dispatch[1], re.M)
    need(labels == list(rows), 'dispatch_table_mismatch', 1)
    need(BASE | INTERACTIONS | {'M%02d' % n for n in range(1, 22)} <= set(rows), 'mandatory_arm_missing', 1)
    # origin: hot-update S8 plan §5.2 / §9.2. Cold boot must prove the
    # rolled-back selector before another upgrade; booting the new image alone
    # would leave the readonly installation contract unexercised.
    need(all(row['operations'] == 'prepare,stage,upgrade,rollback,cold-boot,upgrade-again' and row['kind'] == 'recovery'
             for row in rows.values() if row['family'] == 'H'), 'readonly_reverse_step_missing', 1)
    revocation = rows['I-late-revoke-current-rollback']
    # origin: hot-update S8 plan §8.2. Revocation still wins when a current
    # reverse cut is possible; the new request must retain permission rc 77.
    need((revocation['expected_phase'], revocation['expected_outcome'], revocation['cli_rc'], revocation['kind']) ==
         ('rejected', 'rejected', '77', 'refusal'), 'revocation_priority_weakened', 1)
    cases_source = [line for line in Path(cases_path).read_text().splitlines() if not line.startswith('#')]
    cases = {row['drill_arm']: row for row in csv.DictReader(io.StringIO('\n'.join(cases_source)), delimiter='\t')}
    faults = [row for row in rows.values() if row['family'] == 'W']
    need({row['point'] for row in faults} == {'W%02d' % n for n in range(13)}, 'live_window_missing', 1)
    need({cases[row['case_id']]['fault'] for row in faults} == {'F%d' % n for n in range(10)}, 'live_fault_missing', 1)
    need({row['role'] for row in faults} == {'agent', 'single', 'sole-voter', 'N3'}, 'live_role_missing', 1)
    for row in faults:
        need(row['case_id'] == row['arm'] and row['case_id'] in cases, 'fault_case_missing', 1)
        case = cases[row['case_id']]
        need(not case['reason_if_inapplicable'], 'inapplicable_case_cannot_sign_live_hit', 1)
        for local, upstream in (('point', 'window'), ('role', 'role'), ('expected_phase', 'expected_phase'),
                                ('expected_outcome', 'expected_outcome'), ('cli_rc', 'expected_rc'),
                                ('proofs', 'proof_required'), ('test', 'test')):
            need(row[local] == case[upstream], 'fault_expectation_changed_' + local, 1)
    for fault in ('F5', 'F7'):
        timing = {cases[row['case_id']]['release_timing'] for row in faults if cases[row['case_id']]['fault'] == fault}
        need(timing == {'inside', 'outside'}, 'deadline_lane_missing', 1)
    need({'L', 'D'} == {cases[row['case_id']]['owner_lane'] for row in faults}
         and {'known', 'unknown'} == {cases[row['case_id']]['current_owner'] for row in faults}, 'owner_lane_missing', 1)
    return rows


def load_fixture(lib):
    # -I removes sibling imports from sys.path. Resolve the owned P26 contract
    # explicitly, without relying on the caller's cwd or PYTHONPATH.
    spec = importlib.util.spec_from_file_location('upgrade_tx_fixture', Path(lib) / 'upgrade_tx_fixture.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def safe_path(value):
    path = Path(value)
    need(path.is_absolute() and '.tether' not in path.parts and '/.local/bin/tether' not in str(path)
         and '.tether' not in path.resolve().parts and '/.local/bin/tether' not in str(path.resolve()), 'production_or_relative_path_forbidden')
    return path


def command_spec(spec, supervisor, shared):
    supervisor.check_spec(spec)
    argv = spec['argv']
    need(argv[:2] == [shared, '--'] and len(argv) > 2 and Path(argv[2]).is_absolute(), 'shared_supervisor_required')
    need(not any(arg.startswith(('TETHER_FAULT', '_TETHER_FAULT')) for arg in argv), 'production_fault_switch_forbidden')


def envelope(document, row, fixture, shared):
    C, S = fixture.collect, fixture.supervisor
    fields = {'version', 'arm', 'pair', 'fixture', 'operations', 'mutation_patch_digest'}
    if row['family'] == 'M':
        fields.add('mutation')
    need(set(document) == fields
         and type(document['version']) is int and document['version'] == 1 and document['arm'] == row['arm'], 'runner_manifest_invalid')
    need(Path(shared).is_absolute() and os.access(shared, os.X_OK), 'shared_supervisor_unavailable')
    manifest = fixture.manifest_read(safe_path(document['fixture']))
    pair = C.read(safe_path(document['pair']))
    need(pair['schema_version'] == 1 and pair['a']['commit'] != pair['b']['commit']
         and pair['a']['binary_sha256'] != pair['b']['binary_sha256'], 'distinct_s8_pair_required')
    need(len(set(pair['implementation_digests'])) == 2
         and len({p['binary_sha256'] for p in pair['compiled_implementation_proofs']}) == 2, 'compiled_pair_change_required')
    for artifact in (pair['a'], pair['b']):
        need(artifact['toolchain'] and artifact['build_flags'] and artifact['dirty_diff_sha256']
             and artifact['source_archive'] and artifact['archive_sha256'], 'artifact_provenance_missing')
    need(manifest['arm'] == row['arm'] and manifest['role'] == row['role']
         and manifest['topology'] == row['topology'] and manifest['case_id'] == (row['arm'] if row['case_id'] == 'none' else row['case_id']), 'fixture_arm_mismatch')
    # origin: hot-update S8 plan §9.0 / §10. This is the composite full-profile
    # drill; a smaller manifest cannot silently erase a data-plane obligation.
    need(set(manifest['domains']) == C.DOMAINS, 'fixture_domain_missing')
    # origin: hot-update S8 plan §9.0 / §11.2 P27. Independent membership
    # checks would accept A's bytes carrying B's source provenance.
    artifacts = [pair['a'], pair['b']]
    if row['family'] == 'M':
        mutation = C.mutation_tools().validate_mutation_build(safe_path(document['mutation']), row)
        need(mutation['artifact'] in artifacts and mutation['patch_digest'] == document['mutation_patch_digest'],
             'mutation_not_built_from_pair')
        if mutation['recipe']['build'] != 'python':
            artifacts = [dict(mutation['artifact'], binary_sha256=mutation['binary_sha'])]
    need((manifest['artifact_sha'], manifest['source_commit']) in
         {(artifact['binary_sha256'], artifact['commit']) for artifact in artifacts}, 'fixture_pair_mismatch')
    expected = manifest['expect']
    need((expected['phase'], expected['outcome'], expected['native_rc']) ==
         (row['expected_phase'], row['expected_outcome'], int(row['cli_rc'])), 'fixture_expected_result_changed')
    need(expected['a6'] == (row['family'] == 'A') and expected['hit_required'] == (row['family'] == 'W'), 'fixture_required_oracle_changed')
    need(expected['completion'] == (row['expected_phase'] in {'completed', 'rolled_back'}), 'fixture_completion_weakened')
    need(manifest['deadline_s'] <= 2 * int(row['worst']), 'fixture_exceeds_arm_ceiling')
    operations = document['operations']
    need(isinstance(operations, list) and [step['name'] for step in operations] == row['operations'].split(','), 'scenario_operations_changed')
    for step in operations:
        need(set(step) == {'name', 'command'}, 'scenario_command_fields_invalid')
        if step['name'] not in {'inject', 'release'}:
            command_spec(step['command'], S, shared)
        else:
            need(step['command'] is None and row['family'] == 'W', 'fault_must_use_p26_injector')
    for spec in list(manifest['ports'].values()) + [actor['command'] for actor in manifest['actors']]:
        command_spec(spec, S, shared)
    for fault in manifest['faults']:
        for op in ('arm', 'release'):
            command_spec(fault[op], S, shared)
    need(re.fullmatch('[0-9a-f]{64}', document['mutation_patch_digest']) if row['family'] == 'M'
         else document['mutation_patch_digest'] == 'none', 'mutation_identity_missing_or_unexpected')
    return manifest


def rollback_checkpoint(point, baseline, sample):
    # origin: hot-update S8 plan §8.1 / §11.2 P27. Preflight errno cannot
    # substitute for partial Take. Independent custody must show both taken
    # and remaining original resources before salvage/current reverse.
    if point == 'R1':
        original = {r['id'] for r in baseline['resources']}
        resources = [r for r in sample['resources'] if r['id'] in original]
        target = [r for r in resources if r['current_owner'] != r['origin_owner']]
        remaining = [r for r in resources if r['current_owner'] == r['origin_owner']]
        need(sample['running_sha'] != baseline['running_sha'] and sample['direction'] == 'forward'
             and target and remaining and not sample['resources_receipt']['complete'], 'r1_partial_take_not_hit', 4)
    elif point == 'R2':
        original = {r['id'] for r in baseline['resources']}
        need(sample['running_sha'] != baseline['running_sha'] and sample['direction'] == 'forward'
             and sample['sequence'] > baseline['sequence'] and any(r['id'] not in original for r in sample['resources']),
             'r2_new_request_not_observed', 4)


def rollback_result(point, baseline, checkpoint, final):
    if point == 'R0':
        need(final['running_sha'] == baseline['running_sha'] and final['pid'] == baseline['pid']
             and final['start_ticks'] == baseline['start_ticks'] and final['owner_boot_id'] == baseline['owner_boot_id'], 'r0_original_owner_changed', 3)
        return
    need(checkpoint is not None, 'rollback_checkpoint_missing', 4)
    need(final['direction'] == 'reverse' and final['running_sha'] == baseline['running_sha']
         and final['cut_id'] != checkpoint['cut_id'] and final['generation'] == checkpoint['generation'] + 1, 'reverse_reused_forward_attempt', 1)
    # P26 validates each receipt's nonce/producer/scope and admission barrier.
    # This additional comparison prevents a valid forward triple signing reverse.
    for field in ('control_receipt', 'resources_receipt', 'released_receipt'):
        need(final[field]['evidence_digest'] != checkpoint[field]['evidence_digest'], 'forward_receipt_reused', 1)


def native_status(child_rc, expected_rc):
    # origin: hot-update S8 plan §9.2 / §11.2 P27. Preserve the child rc in
    # its own receipt. A known product refusal passes only with its full oracle.
    need(type(child_rc) is int and 0 <= child_rc <= 255, 'command_native_rc_invalid')
    return 0 if child_rc == expected_rc else 3


def join_native(statuses):
    # Same precedence as assert.sh; an expected mutant never becomes GREEN.
    return next((rc for rc in (1, 2, 3, 4) if rc in statuses), 0)


def checked_empty(empty):
    # origin: hot-update S8 plan §9.0 / §11.2 P27. Only the shared fixture
    # supervisor's postcondition can discharge child custody after a failure.
    need(empty['empty'] is True and empty['remaining'] == [], 'fixture_not_empty_after_stop')


def calibrate(sample, deadline, clock=time.monotonic, sleep=time.sleep):
    # origin: hot-update S8 plan §10. A command ACK starts a calibration;
    # it cannot replace the actual 300-second interval with a phase marker.
    until = clock() + 300
    need(until <= deadline, 'calibration_does_not_fit_arm_deadline', 4)
    while clock() < until:
        need(clock() < deadline, 'arm_absolute_deadline_exceeded', 4)
        sample()
        # sleep-fixture: sampling continues through the full pre-cut interval.
        sleep(max(0, min(5, until - clock())))
    sample()


def checked_judgment(result, row, manifest, originals):
    need(type(result['rc']) is int and result['rc'] in VERDICTS, 'scenario_judge_invalid_native_rc')
    need(isinstance(result.get('scenario'), dict), 'scenario_companion_receipts_not_wired')
    proof = result['scenario']
    need(set(proof) == {'version', 'run_id', 'arm', 'source_commit', 'original_set_digest', 'proofs', 'rc'}
         and type(proof['version']) is int and proof['version'] == 1, 'scenario_judge_schema_invalid')
    need(proof['run_id'] == manifest['run_id'] and proof['arm'] == row['arm']
         and proof['source_commit'] == manifest['source_commit']
         and proof['original_set_digest'] == originals['original_set_digest'], 'scenario_judge_stale', 1)
    need(type(proof['rc']) is int and proof['rc'] == result['rc'], 'scenario_judge_rc_laundered', 1)
    need(set(proof['proofs']) == set(row['proofs'].split(',')), 'scenario_proof_missing', 4)
    # Mutation red remains red. If a mutated artifact runs green, its declared
    # negative control did not fire and the runner itself becomes ASSERT-RED.
    if row['family'] == 'M':
        need(result['rc'] == int(row['expected_native']), 'mutant_did_not_turn_red', 1)
    return result['rc']


def mutation_native(row, statuses, verified):
    native = join_native(statuses)
    if row['family'] == 'M' and not verified:
        return 2 if 2 in statuses else 4
    return native


def execute_arm(row, manifest, document, evidence, fixture):
    C, S = fixture.collect, fixture.supervisor
    root = Path(evidence)
    reader, writer = os.pipe()
    statuses, commands, checkpoint = [], [], None
    mutant_verified = False
    started = False
    deadline = time.monotonic() + min(manifest['deadline_s'], 2 * int(row['worst']))
    reason = 'all_declared_oracles_observed'
    def observed(call):
        try:
            return call()
        except C.EvidenceError as error:
            if row['family'] != 'M' or error.rc not in {1, 3}:
                raise
            # The independent Go companion still needs a chance to inspect
            # this mutant. Preserve the primary error without certifying it.
            statuses.append(error.rc)
            return None
    try:
        fixture.start(root, manifest, reader)
        started = True
        os.close(reader)
        reader = -1
        C.publish(root / 'runner-envelope.json', document)
        C.publish(root / 'arm-contract.json', row)
        baseline = C.read(root / 'start-observation.json')
        originals = C.read(root / 'originals.json')
        faults_spec = importlib.util.spec_from_file_location('upgrade_tx_faults', Path(fixture.HERE) / 'upgrade_tx_faults.py')
        faults = importlib.util.module_from_spec(faults_spec)
        faults_spec.loader.exec_module(faults)
        for ordinal, step in enumerate(document['operations']):
            need(time.monotonic() < deadline, 'arm_absolute_deadline_exceeded', 4)
            name = step['name']
            if name in {'inject', 'release'}:
                result = getattr(faults, 'arm' if name == 'inject' else 'release')(root, manifest, manifest['case_id'])
                native = result['rc']
                output = json.dumps(result)
                expected = 0
            else:
                result = S.call(root, dict(op='run', spec=step['command'], input=dict(version=1, arm=row['arm'],
                         operation=name, evidence=str(root), original_set_digest=originals['original_set_digest'])))
                native, output = result['native_rc'], result['stdout']
                expected = int(row['cli_rc']) if ordinal == len(document['operations']) - 1 else 0
            command = dict(ordinal=ordinal, operation=name, argv=None if step['command'] is None else step['command']['argv'],
                           native_rc=native, expected_rc=expected, stdout=output)
            commands.append(command)
            C.publish(root / ('command-%02d.json' % ordinal), command)
            statuses.append(native_status(native, expected))
            # A destructive arm can kill its mutated owner. Capture its
            # actual running bytes after prepare, before the destructive cut;
            # the P25 oracle separately binds the subsequent fault evidence.
            if row['family'] == 'M' and (root / 'mutation-owner.json').is_file() and not (root / 'mutation-execution.json').exists():
                faults.observe_mutation(root, manifest, row, document['mutation'])
            if row['family'] == 'A' and name in {'calibrate-idle', 'calibrate-full'} and not statuses[-1]:
                calibrate(lambda: C.sample(root, manifest['case_id'], secrets.token_hex(16)), deadline)
            sample = observed(lambda: C.sample(root, manifest['case_id'], secrets.token_hex(16)))
            if row['family'] == 'R' and name in {'partial-take', 'new-request'}:
                rollback_checkpoint(row['point'], baseline, sample)
                checkpoint = sample
            if statuses[-1]:
                # Keep all raw output and still inspect/drain/stop the original
                # fixture. Never stage a replacement workload to repair a failure.
                reason = 'scenario_command_native_failure'
                break
        if row['family'] == 'M' and not (root / 'mutation-execution.json').exists():
            faults.observe_mutation(root, manifest, row, document['mutation'])
        final = observed(lambda: C.sample(root, manifest['case_id'], secrets.token_hex(16)))
        if row['family'] == 'R':
            rollback_result(row['point'], baseline, checkpoint, final)
        observed(lambda: fixture.drain(root, manifest))
        try:
            C.produce_companion(root, manifest, row, document['operations'][0]['command']['argv'][0],
                                max(1, deadline - time.monotonic()))
            judged = C.judge(root, manifest)
            C.publish(root / 'judge.json', judged)
            statuses.append(checked_judgment(judged, row, manifest, originals))
            mutant_verified = (row['family'] == 'M' and judged['rc'] == int(row['expected_native'])
                               and all(c['native_rc'] == c['expected_rc'] for c in commands))
        except C.EvidenceError as error:
            statuses.append(error.rc)
            reason = str(error)
            C.publish(root / 'judge.json', dict(rc=error.rc, reason=str(error)))
    except (ContractError, C.EvidenceError) as error:
        statuses.append(error.rc)
        reason = str(error)
    except Exception:
        # Unclassified runner failures are setup failures. Python's default
        # exit 1 would otherwise masquerade as an expected M-arm assertion.
        statuses.append(2)
        reason = 'live_contract_unavailable'
    finally:
        if reader >= 0:
            os.close(reader)
        # Keep the only lifeline writer through judge and cleanup. EOF is the
        # fallback if stop fails or the runner is killed; it cannot certify empty.
        try:
            if started:
                empty = fixture.stop(root, manifest)
                checked_empty(empty)
                C.publish(root / 'runner-empty.json', empty)
        except Exception:
            statuses.append(2)
            mutant_verified = False
            reason = 'fixture_cleanup_not_proven'
        finally:
            os.close(writer)
    native = mutation_native(row, statuses, mutant_verified)
    # origin: hot-update S8 plan §8.3 / F8. Generic assertion red is not a
    # killed mutant. Preserve raw child statuses, but keep this arm incomplete
    # until its exact independent native oracle has validated the build/run.
    if row['family'] == 'M' and not mutant_verified:
        reason = 'mutation_causal_receipt_missing:' + reason
    receipt = dict(version=1, arm=row['arm'], unit='38-hot-update-rollback-compat.' + row['arm'],
                   native_rc=native, expected_native=int(row['expected_native']), verdict=VERDICTS[native],
                   kind=row['kind'], reason=reason, commands=commands, child_statuses=statuses,
                   source_commit=manifest['source_commit'], artifact_sha=manifest['artifact_sha'],
                   run_id=manifest['run_id'], manifest_digest=C.digest(manifest), envelope_digest=C.digest(document),
                   mutation_patch_digest=document['mutation_patch_digest'], worst_s=int(row['worst']),
                   mutant_verified=mutant_verified,
                   ceiling_s=2 * int(row['worst']))
    if root.is_dir():
        C.publish(root / 'arm-receipt.json', receipt)
    return receipt


def emit_receipt(result):
    # origin: hot-update S8 plan §9.2 / §11.2 P27. The shell must see the
    # same native status as the persisted arm receipt, including mutant red.
    print(json.dumps(result, sort_keys=True))
    return result['native_rc']


def main(arguments):
    script, arms_path, cases_path, lib, arm, manifest_path, evidence = arguments
    rows = contract(script, arms_path, cases_path)
    need(arm in rows, 'unknown_arm')
    fixture = load_fixture(lib)
    safe_path(evidence)
    document = fixture.collect.read(safe_path(manifest_path))
    manifest = envelope(document, rows[arm], fixture, os.environ['SIM_UPGRADE_SUPERVISOR'])
    result = execute_arm(rows[arm], manifest, document, evidence, fixture)
    return emit_receipt(result)


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv[1:]))
    except ContractError as error:
        print(json.dumps(dict(native_rc=error.rc, verdict=VERDICTS[error.rc], reason=str(error))))
        sys.exit(error.rc)
    except Exception:
        print('{"native_rc":2,"verdict":"SETUP-RED","reason":"live_contract_unavailable"}')
        sys.exit(2)
PY38
}

upgrade38_native() {
    case "$1" in
        0) _as_pass "38.$ARM full declared oracle and empty fixture" ;;
        1) _as_fail "38.$ARM native ASSERT-RED" ;;
        2) _as_setup_red "38.$ARM native SETUP-RED" ;;
        3) _as_product_red "38.$ARM native PRODUCT-RED" ;;
        4) not_covered "38.$ARM native INCOMPLETE" "required live interval/proof was not observed" gap ;;
        *) _as_setup_red "38.$ARM supervisor/runner failure (native rc $1)" ;;
    esac
}

: "${HERE:?simcluster root is required}"
SIM_REPO=${SIM_REPO:-$(CDPATH= cd -- "$HERE/../.." && pwd)}
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
. "$HERE/lib/supervisor.sh"
S8_PYTHON=${S8_PYTHON:-/usr/bin/python3}
drill_begin "38-hot-update-rollback-compat [arm ${ARM:-?}]"
[ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || setup_fail "shared process supervisor is required"
[ -n "${SIM_UPGRADE_MANIFEST:-}" ] && [ -f "$SIM_UPGRADE_MANIFEST" ] || setup_fail "P22/P23/P25/P26 live arm manifest is required"
[ -n "${SIM_UPGRADE_EVIDENCE:-}" ] || setup_fail "one fresh evidence directory per arm is required"
# Only one flat child runs here. The shared runner alone joins all declarations.
case "${ARM:?}" in
    A-single) upgrade38_run ;;
    A-sole) upgrade38_run ;;
    A-n3) upgrade38_run ;;
    R0) upgrade38_run ;;
    R1) upgrade38_run ;;
    R2) upgrade38_run ;;
    H-root) upgrade38_run ;;
    H-user) upgrade38_run ;;
    H-no-systemd) upgrade38_run ;;
    H-colocated) upgrade38_run ;;
    H-sibling) upgrade38_run ;;
    H-readonly) upgrade38_run ;;
    G-two) upgrade38_run ;;
    G-third) upgrade38_run ;;
    G-concurrent) upgrade38_run ;;
    C-driver) upgrade38_run ;;
    C-reopen) upgrade38_run ;;
    C-gc) upgrade38_run ;;
    C-late-writer) upgrade38_run ;;
    C-history-rollback) upgrade38_run ;;
    C-owner-dead) upgrade38_run ;;
    W00-F0-agent-L-known-none) upgrade38_run ;;
    W01-F1-single-L-known-none) upgrade38_run ;;
    W02-F2-sole-voter-L-known-none) upgrade38_run ;;
    W03-F3-N3-L-known-none) upgrade38_run ;;
    W04-F4-agent-L-known-none) upgrade38_run ;;
    W05-F5-single-L-known-inside) upgrade38_run ;;
    W06-F6-sole-voter-L-known-none) upgrade38_run ;;
    W07-F7-N3-L-known-inside) upgrade38_run ;;
    W08-F0-agent-D-known-none) upgrade38_run ;;
    W09-F1-single-D-known-none) upgrade38_run ;;
    W10-F2-sole-voter-D-known-none) upgrade38_run ;;
    W11-F8-N3-D-known-none) upgrade38_run ;;
    W12-F9-N3-D-known-none) upgrade38_run ;;
    W05-F5-single-L-known-outside) upgrade38_run ;;
    W07-F7-N3-L-known-outside) upgrade38_run ;;
    W09-F5-agent-D-known-inside) upgrade38_run ;;
    W09-F5-agent-D-known-outside) upgrade38_run ;;
    W09-F7-N3-D-known-inside) upgrade38_run ;;
    W09-F7-N3-D-known-outside) upgrade38_run ;;
    W08-F0-single-D-unknown-none) upgrade38_run ;;
    W09-F1-agent-D-unknown-none) upgrade38_run ;;
    W12-F9-N3-D-unknown-none) upgrade38_run ;;
    W08-F3-single-D-known-none) upgrade38_run ;;
    W09-F4-sole-voter-D-known-none) upgrade38_run ;;
    W09-F6-agent-D-known-none) upgrade38_run ;;
    I-late-revoke-current-rollback) upgrade38_run ;;
    I-nats-cut-leader-move) upgrade38_run ;;
    I-begin-archive-gc) upgrade38_run ;;
    I-driver-death-host-reservation) upgrade38_run ;;
    M01) upgrade38_run ;;
    M02) upgrade38_run ;;
    M03) upgrade38_run ;;
    M04) upgrade38_run ;;
    M05) upgrade38_run ;;
    M06) upgrade38_run ;;
    M07) upgrade38_run ;;
    M08) upgrade38_run ;;
    M09) upgrade38_run ;;
    M10) upgrade38_run ;;
    M11) upgrade38_run ;;
    M12) upgrade38_run ;;
    M13) upgrade38_run ;;
    M14) upgrade38_run ;;
    M15) upgrade38_run ;;
    M16) upgrade38_run ;;
    M17) upgrade38_run ;;
    M18) upgrade38_run ;;
    M19) upgrade38_run ;;
    M20) upgrade38_run ;;
    M21) upgrade38_run ;;
    *) setup_fail "unknown upgrade transaction arm '$ARM'" ;;
esac
S8_NATIVE=$?
upgrade38_native "$S8_NATIVE"
drill_end
