"""On-disk mutation discovery and controls for the current S8 collector roots."""
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

# origin: hot-update S7 plan §11.4 / T50
# gate-control: UpgradeCollectorSourceRoots
COLLECTOR_SOURCES = ('upgrade_tx_collect.py', 'upgrade_tx_evidence.py', 'upgrade_tx_companion.py')


def collector_sources():
    return {LIB/name: (LIB/name).read_text() for name in COLLECTOR_SOURCES}


def collector_guards(sources):
    guards = []
    for path, original in sources.items():
        offsets = [0]
        for line in original.splitlines(True):
            offsets.append(offsets[-1] + len(line))
        # Producer/selection controls remain in drill38-contract-test.sh.
        roots = [node for node in ast.parse(original).body if isinstance(node, ast.FunctionDef)
                 and node.name not in {'companion_selection', 'companion_corpus', 'produce_companion'}]
        for node in (child for root in roots for child in ast.walk(root)):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name) or node.func.id != 'require' or len(node.args)<2:
                continue
            reason = node.args[1]
            if not isinstance(reason, ast.Constant) or not isinstance(reason.value, str):
                continue
            if not reason.value.startswith(('scenario_', 'archive_', 'cold_boundary_')):
                continue
            argument = node.args[0]
            start = offsets[argument.lineno-1] + argument.col_offset
            end = offsets[argument.end_lineno-1] + argument.end_col_offset
            guards.append((path, reason.value, node.lineno, start, end))
    return guards


# origin: hot-update S7 plan §11.4 / T50
class UpgradeCollectorSourceRoots(unittest.TestCase):
    def test_mutation_scanner_follows_moved_guards(self):
        sources = collector_sources()
        self.assertEqual({path.name for path in sources}, set(COLLECTOR_SOURCES))
        for path in sources:
            self.assertLessEqual(len(sources[path].splitlines()), 700)
        guards = collector_guards(sources)
        for filename, reason in [('upgrade_tx_collect.py', 'archive_custody_not_empty'),
                                 ('upgrade_tx_evidence.py', 'scenario_evidence_path_foreign'),
                                 ('upgrade_tx_companion.py', 'scenario_validation_test_not_passed')]:
            selected = [row for row in guards if row[0].name == filename and row[1] == reason]
            self.assertEqual(len(selected), 1, (filename, reason))
            path, _, _, start, end = selected[0]
            self.assertNotEqual(sources[path][start:end], 'True')
        # A live negative sample in every moved root must also be selected.
        for path, source in sources.items():
            injected = dict(sources)
            injected[path] = source + '\ndef negative_control():\n    require(False, "scenario_control", 1)\n'
            self.assertEqual(sum(row[0] == path and row[1] == 'scenario_control'
                                 for row in collector_guards(injected)), 1)
        self.assertEqual(collector_guards({LIB/'empty.py': 'pass\n'}), [])


def mutation_checks(group):
    # origin: hot-update S8 plan §9.0 / §11.2 P26b. Each group stays below
    # the existing 55-second supervised command cap. Mutations are ON DISK;
    # every child imports fresh source and finally restores the exact bytes.
    sources = collector_sources()
    mutations = []
    # Producer/selection controls live in drill38-contract-test.sh. This
    # partition mutates only guards exercised by UpgradeScenarioOracle.
    for path, reason, line, start, end in collector_guards(sources):
        original = sources[path]
        test = ('test_cold_boot_boundary_is_explicit_and_cannot_reconnect_originals' if reason.startswith('cold_')
                else 'test_owner_death_archive_requires_independent_empty_custody' if reason.startswith('archive_')
                else 'test_scenario_preserves_native_mutant_red' if reason == 'scenario_mutant_not_executed' or reason.startswith('scenario_mutation_')
                else 'test_companion_native_invocation_cannot_be_relabelled' if reason.startswith(('scenario_invocation_', 'scenario_verifier_'))
                else 'test_scenario_declaration_cannot_remove_required_evidence' if reason in
                    {'scenario_arm_not_declared','scenario_expectation_changed','scenario_cold_boundary_missing','scenario_a6_oracle_disabled'}
                else 'test_scenario_requires_bound_companion_files_and_native_validation')
        mutations.append((reason+'-'+path.stem+'-line-'+str(line), path, original[:start]+'True'+original[end:], test))
    extras = [
        ('phase-implies-unknown','arm = manifest["arm"]','return manifest["expect"]["outcome"] == "unknown"\n    arm = manifest["arm"]',
         'test_unknown_outcome_does_not_declare_unknown_ownership'),
        ('archive-lane-omitted','return arm == "C-owner-dead" or','return False or','test_owner_death_archive_requires_independent_empty_custody'),
        ('scenario-output-omitted','result["scenario"] = scenario_result(manifest, originals, proofs, result["rc"])','pass',
         'test_judge_emits_closed_scenario_consumed_by_drill'),
        ('native-mutant-laundered','result["verdict"] = VERDICTS[result["rc"]]','result["rc"] = 0\n        result["verdict"] = VERDICTS[0]',
         'test_scenario_preserves_native_mutant_red'),
        ('archive-unsettled-drain','settled=latest["phase"] == "abandoned"','settled=False','test_owner_death_archive_requires_independent_empty_custody'),
        ('cold-originals-counted-as-live','expected = {key: value for key, value in expected.items() if key not in retired}',
         'expected = expected','test_cold_boot_boundary_is_explicit_and_cannot_reconnect_originals'),
        ('cold-boundary-applies-before-boot','if sample["observed_end_ns"] < boundary["begin_ns"]:','if False:',
         'test_cold_boot_boundary_is_explicit_and_cannot_reconnect_originals'),
        ('empty-cold-census-completes','require(expected, "receipt_live_census_empty", 1)','require(True, "receipt_live_census_empty", 1)',
         'test_cold_boot_boundary_is_explicit_and_cannot_reconnect_originals'),
    ]
    for name,needle,replacement,test in extras:
        matches = [path for path, original in sources.items() if needle in original]
        if len(matches)!=1 or sources[matches[0]].count(needle)!=1:
            raise AssertionError('mutation anchor not unique: '+name)
        path = matches[0]
        mutations.append((name,path,sources[path].replace(needle,replacement,1),test))
    chosen = mutations[group*12:(group+1)*12]
    if not chosen: raise AssertionError('empty mutation partition')
    receipts = Path(tempfile.mkdtemp(prefix='s8-p26b-mutations-'+str(group)+'-', dir=os.environ.get('S8_RECEIPTS_DIR')))
    results = []
    try:
        for name,path,changed,test in chosen:
            original = sources[path]
            path.write_text(changed)
            try:
                result = S.run(['/bin/sh',str(LIB.parent.parent/'tests/upgrade-fixture-test.sh'),'UpgradeScenarioOracle.'+test],15)
                patch = ''.join(difflib.unified_diff(original.splitlines(True),changed.splitlines(True),fromfile=str(path),tofile=str(path)))
                (receipts/(name+'.patch')).write_text(patch)
                (receipts/(name+'.log')).write_text(result['stdout'])
                row = dict(mutation=name,native_rc=result['native_rc'],patch_sha256=hashlib.sha256(patch.encode()).hexdigest(),test=test)
                results.append(row)
                print(json.dumps(row),flush=True)
                if result['native_rc']!=1: raise AssertionError('mutation survived: '+name)
            finally:
                path.write_text(original)
    finally:
        for path, original in sources.items():
            path.write_text(original)
        (receipts/'summary.json').write_text(json.dumps(results,indent=2)+'\n')
        print('Mutation receipts: '+str(receipts),flush=True)
    result = S.run(['/bin/sh',str(LIB.parent.parent/'tests/upgrade-fixture-test.sh'),'UpgradeScenarioOracle'],15)
    (receipts/'restored-positive.log').write_text(result['stdout'])
    print(json.dumps(dict(restored_positive_rc=result['native_rc'],mutants=len(results),total_mutants=len(mutations))),flush=True)
    if result['native_rc']!=0: raise AssertionError('restored positive failed')


