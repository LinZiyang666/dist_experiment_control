#!/bin/sh
# origin: hot-update S8 plan §9.2 / §11.2 P27. Hermetic contract controls only.
# Synthetic rows exercise guards; no product, cluster or deploy arm is started.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
SIM_REPO=$(CDPATH= cd -- "$HERE/../.." && pwd)
. "$HERE/lib/supervisor.sh"
S8_TEST_TMP=$(mktemp -d "${TMPDIR:-/tmp}/drill38-contract.XXXXXX")
trap 'rm -rf "$S8_TEST_TMP"' EXIT HUP INT TERM
sim_supervisor_provision "$S8_TEST_TMP"
export S8_TEST_SIM_ROOT="$HERE" S8_TEST_REPO="$SIM_REPO"
S8_TEST_RC=0
"$SIM_UPGRADE_SUPERVISOR" -- /usr/bin/python3 -I -B - "$@" <<'PY38TEST' || S8_TEST_RC=$?
import copy
import contextlib
import csv
import difflib
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tarfile
from unittest import mock
import types
import unittest

ROOT = Path(os.environ['S8_TEST_SIM_ROOT'])
REPO = Path(os.environ['S8_TEST_REPO'])
SHARED = os.environ['SIM_UPGRADE_SUPERVISOR']
SCRIPT = ROOT / 'drills/38-hot-update-rollback-compat.sh'
ARMS = ROOT / 'drills/lib/upgrade_tx_arms.tsv'
CASES = REPO / 'test/hotupdate/testdata/upgrade_fault_cases.tsv'
SOURCE = SCRIPT.read_text()
python = SOURCE.split("<<'PY38'\n", 1)[1].split('\nPY38\n', 1)[0]
D = types.ModuleType('drill38_contract')
# __name__ is deliberately not __main__: execute definitions, never the drill.
exec(compile(python, str(SCRIPT), 'exec'), D.__dict__)
ROWS = D.contract(SCRIPT, ARMS, CASES)
F = D.load_fixture(ROOT / 'drills/lib')
C = F.collect


def rewritten_headers(source, rows):
    for key in ('arms', 'fixture', 'grows', 'worst', 'forgoes'):
        value = ' '.join(rows) if key == 'arms' else ' '.join(a + '=' + r[key] for a, r in rows.items())
        source = re.sub(r'^# ' + key + r': .*$', '# ' + key + ': ' + value, source, flags=re.M)
    return source


def scratch_contract(rows=None, source=None):
    # Scanner controls use on-disk files, not overlays or in-memory substitutions.
    rows = copy.deepcopy(ROWS if rows is None else rows)
    with tempfile.TemporaryDirectory(prefix='drill38-source-control-') as directory:
        root = Path(directory)
        script, arms = root / 'drill.sh', root / 'arms.tsv'
        script.write_text(SOURCE if source is None else source)
        with arms.open('w') as stream:
            writer = csv.DictWriter(stream, fieldnames=D.FIELDS, delimiter='\t', lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows.values())
        return D.contract(script, arms, CASES)


def checkpoint():
    old, new = dict(pid=10, start_ticks=50), dict(pid=10, start_ticks=50, instance_id='new-runtime')
    original = [dict(id=name, origin_owner=old, current_owner=old) for name in ('exec.original', 'flow.original')]
    baseline = dict(running_sha='a' * 64, pid=10, start_ticks=50, owner_boot_id='boot', sequence=5, resources=original)
    current = dict(running_sha='b' * 64, direction='forward', cut_id='new-cut', generation=2, sequence=6,
                   resources=[original[0], dict(original[1], current_owner=new)],
                   resources_receipt=dict(complete=False, evidence_digest='forward-resources'),
                   control_receipt=dict(evidence_digest='forward-control'),
                   released_receipt=dict(evidence_digest='forward-released'))
    return baseline, current


def judgment():
    row = ROWS['R1']
    manifest = dict(run_id='run', source_commit='c' * 40)
    originals = dict(original_set_digest='d' * 64)
    proof = dict(version=1, run_id='run', arm='R1', source_commit='c' * 40,
                 original_set_digest='d' * 64, proofs=row['proofs'].split(','), rc=0)
    return row, manifest, originals, proof


def fixture_manifest(root):
    # Only a pure validator input. Actors/ports are never executed by this test.
    spec = dict(argv=[SHARED, '--', '/usr/bin/true'], cwd=str(root), timeout_s=5)
    actors = [dict(id=family + str(n), family=family, resource_ids=[family + str(n)], command=copy.deepcopy(spec))
              for family, count in C.PROFILE.items() for n in range(count)]
    expected = dict(phase='completed', outcome='success', native_rc=0, completion=True, hit_required=False, a6=True)
    manifest = dict(version=1, source_commit='c' * 40, artifact_sha='b' * 64, run_id='1' * 32,
                    arm='A-single', case_id='A-single', role='single', topology='single', actors=actors,
                    ports={name:copy.deepcopy(spec) for name in ('observe', 'admissions', 'drain')},
                    domains=sorted(C.DOMAINS), expect=expected, faults=[], deadline_s=1100,
                    transfer_budget=dict(rate_bytes_s=1024, cut_budget_s=1100, drain_budget_s=10, margin_s=10, size_bytes=2000000))
    artifact = dict(commit='a' * 40, binary_sha256='a' * 64, toolchain='go1.26.8', build_flags=['-trimpath'],
                    dirty_diff_sha256='d' * 64, source_archive='/tmp/source.tar', archive_sha256='e' * 64)
    other = dict(artifact, commit='c' * 40, binary_sha256='b' * 64)
    pair = dict(schema_version=1, a=artifact, b=other, implementation_digests=['a', 'b'],
                compiled_implementation_proofs=[dict(binary_sha256='a'), dict(binary_sha256='b')])
    (root / 'fixture.json').write_text(json.dumps(manifest))
    (root / 'pair.json').write_text(json.dumps(pair))
    document = dict(version=1, arm='A-single', fixture=str(root / 'fixture.json'), pair=str(root / 'pair.json'),
                    operations=[dict(name=name, command=copy.deepcopy(spec)) for name in ROWS['A-single']['operations'].split(',')],
                    mutation_patch_digest='none')
    return manifest, pair, document


class Drill38ContractTests(unittest.TestCase):
    # origin: hot-update S8 plan §9.2 / P27.
    def test_flat_arms_bind_headers_cases_and_budget(self):
        self.assertEqual(len(ROWS), 71)
        self.assertEqual(sum(int(r['worst']) for r in ROWS.values()), 12570)
        self.assertEqual(sum(r['family'] == 'W' for r in ROWS.values()), 25)
        self.assertEqual({r['arm'] for r in ROWS.values() if r['family'] == 'M'}, {'M%02d' % n for n in range(1, 22)})
        for arm, row in ROWS.items():
            with self.subTest(arm=arm):
                self.assertEqual(row['forgoes'], '-')
                self.assertGreaterEqual(int(row['worst']), 60)
                self.assertLessEqual(2 * int(row['worst']), 14400)
        with tempfile.TemporaryDirectory(prefix='drill38-parse-') as directory:
            # Parse only. Running the drill itself is outside this selftest's scope.
            for shell in ('/bin/sh', '/bin/dash'):
                if Path(shell).is_file():
                    result = subprocess.run([SHARED, '--', shell, '-n', str(SCRIPT)], cwd=directory,
                                            text=True, capture_output=True, timeout=15)
                    self.assertEqual(result.returncode, 0, result.stderr)

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_on_disk_manifest_controls_reject_missing_children_and_case_drift(self):
        for key in ('arms', 'fixture', 'grows', 'worst', 'forgoes'):
            with self.subTest(key=key):
                with self.assertRaises(D.ContractError):
                    scratch_contract(source=re.sub(r'^# ' + key + r': .*\n', '', SOURCE, flags=re.M))
        with self.assertRaises(D.ContractError):
            scratch_contract(source=SOURCE.replace('    R1) upgrade38_run ;;', ''))
        rows = copy.deepcopy(ROWS)
        rows['W08-F0-agent-D-known-none']['cli_rc'] = '75'
        with self.assertRaisesRegex(D.ContractError, 'fault_expectation_changed'):
            scratch_contract(rows, rewritten_headers(SOURCE, rows))
        with self.assertRaises(D.ContractError):
            scratch_contract(source=SOURCE.replace('# arms:', '# arms: R1', 1))
        with self.assertRaises(D.ContractError):
            scratch_contract(source=SOURCE + '\n# worst: R1=60\n')
        rows = copy.deepcopy(ROWS)
        rows['I-late-revoke-current-rollback']['cli_rc'] = '0'
        with self.assertRaisesRegex(D.ContractError, 'revocation_priority_weakened'):
            scratch_contract(rows, rewritten_headers(SOURCE, rows))
        rows = copy.deepcopy(ROWS)
        rows['H-readonly']['operations'] = 'prepare,stage,upgrade,cold-boot,upgrade-again'
        with self.assertRaisesRegex(D.ContractError, 'readonly_reverse_step_missing'):
            scratch_contract(rows)
        rows = copy.deepcopy(ROWS)
        rows['H-root']['kind'] = 'hot'
        with self.assertRaisesRegex(D.ContractError, 'readonly_reverse_step_missing'):
            scratch_contract(rows)

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_r1_requires_partial_take_and_remaining_original_custody(self):
        before, partial = checkpoint()
        D.rollback_checkpoint('R1', before, partial)
        defects = [dict(partial, running_sha=before['running_sha']),
                   dict(partial, direction='reverse'),
                   dict(partial, resources=partial['resources'][:1]),
                   dict(partial, resources=partial['resources'][1:]),
                   dict(partial, resources_receipt=dict(complete=True))]
        # A newly admitted target resource is not evidence that any original
        # descriptor crossed Take. Keep both original owners unchanged.
        defects.append(dict(partial, resources=before['resources'] + [dict(partial['resources'][1], id='new-admission')]))
        for sample in defects:
            with self.subTest(sample=sample):
                with self.assertRaisesRegex(D.ContractError, 'r1_partial_take_not_hit') as failure:
                    D.rollback_checkpoint('R1', before, sample)
                self.assertEqual(failure.exception.rc, 4)

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_r2_requires_new_admission_after_running_image_change(self):
        before, after = checkpoint()
        after['resources'].append(dict(id='new-request', current_owner={}, origin_owner={}))
        D.rollback_checkpoint('R2', before, after)
        for defect in (dict(after, sequence=5), dict(after, running_sha=before['running_sha']),
                       dict(after, resources=before['resources']), dict(after, direction='reverse')):
            with self.assertRaises(D.ContractError):
                D.rollback_checkpoint('R2', before, defect)

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_reverse_cut_and_each_receipt_are_independent(self):
        before, partial = checkpoint()
        final = dict(partial, direction='reverse', running_sha=before['running_sha'], cut_id='reverse-cut', generation=3)
        for field in ('control_receipt', 'resources_receipt', 'released_receipt'):
            final[field] = dict(evidence_digest='reverse-' + field)
        D.rollback_result('R1', before, partial, final)
        for field in ('control_receipt', 'resources_receipt', 'released_receipt'):
            defect = dict(final, **{field:partial[field]})
            with self.subTest(field=field), self.assertRaisesRegex(D.ContractError, 'forward_receipt_reused'):
                D.rollback_result('R1', before, partial, defect)
        for changed in (dict(direction='forward'), dict(generation=2), dict(generation=4), dict(cut_id='new-cut'), dict(running_sha='b' * 64)):
            with self.assertRaises(D.ContractError):
                D.rollback_result('R2', before, partial, dict(final, **changed))
        with self.assertRaises(D.ContractError):
            D.rollback_result('R1', before, None, final)

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_r0_preserves_original_boot_pid_and_image(self):
        before, _ = checkpoint()
        D.rollback_result('R0', before, None, before)
        for field, value in (('running_sha', 'b' * 64), ('pid', 11), ('start_ticks', 51), ('owner_boot_id', 'other')):
            with self.subTest(field=field), self.assertRaises(D.ContractError):
                D.rollback_result('R0', before, None, dict(before, **{field:value}))

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_native_child_status_and_mutant_red_are_preserved(self):
        for child in (1, 2, 3, 4, 64, 74, 75, 77, 125, 137):
            with self.subTest(child=child):
                self.assertEqual(D.native_status(child, 0), 3)
        for expected in (0, 64, 74, 75, 77):
            self.assertEqual(D.native_status(expected, expected), 0)
        for rc in (1, 2, 3, 4):
            self.assertEqual(D.join_native([0, rc, 0]), rc)
        self.assertEqual(D.join_native([4, 3, 2, 1]), 1)
        row, manifest, originals, proof = judgment()
        row = dict(row, family='M', expected_native='1')
        result = dict(rc=0, scenario=proof)
        with self.assertRaisesRegex(D.ContractError, 'mutant_did_not_turn_red'):
            D.checked_judgment(result, row, manifest, originals)
        proof['rc'] = 1
        self.assertEqual(D.checked_judgment(dict(rc=1, scenario=proof), row, manifest, originals), 1)

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_emitted_receipt_preserves_native_exit_status(self):
        # Only the output boundary is exercised. These are runner status rows,
        # never fabricated fixture observations or successful live evidence.
        for rc in range(5):
            receipt = dict(arm='R1', native_rc=rc, expected_native=0, verdict=D.VERDICTS[rc])
            output = io.StringIO()
            with self.subTest(rc=rc), contextlib.redirect_stdout(output):
                self.assertEqual(D.emit_receipt(receipt), rc)
            self.assertEqual(json.loads(output.getvalue()), receipt)

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_judgment_cannot_reuse_another_arm_or_launder_native_rc(self):
        row, manifest, originals, proof = judgment()
        self.assertEqual(D.checked_judgment(dict(rc=0, scenario=proof), row, manifest, originals), 0)
        with self.assertRaisesRegex(D.ContractError, 'scenario_companion_receipts_not_wired'):
            D.checked_judgment(dict(rc=0), row, manifest, originals)
        with self.assertRaisesRegex(D.ContractError, 'scenario_judge_invalid_native_rc'):
            D.checked_judgment(dict(rc=False, scenario=proof), row, manifest, originals)
        for field, changed in (('run_id', 'other'), ('arm', 'R0'), ('source_commit', 'x' * 40),
                               ('original_set_digest', 'x' * 64), ('proofs', proof['proofs'][:-1]), ('rc', 1)):
            with self.subTest(field=field), self.assertRaises(D.ContractError):
                D.checked_judgment(dict(rc=0, scenario=dict(proof, **{field:changed})), row, manifest, originals)

    # origin: hot-update S8 plan §9.0 / P26b. Exercise the actual P26 reader
    # output at this seam; a hand-built scenario cannot prove interoperability.
    def test_actual_fixture_judgment_satisfies_drill_scenario_contract(self):
        source = (ROOT / 'tests/upgrade-fixture-test.sh').read_text()
        definitions = source.split("<<'PY'\n", 1)[1].split('\nPY\n', 1)[0]
        namespace = dict(__name__='fixture_oracle_inputs')
        os.environ['S8_FIXTURE_LIB'] = str(ROOT/'drills/lib')
        with mock.patch.dict(sys.modules):
            exec(compile(definitions, 'fixture-oracle-inputs', 'exec'), namespace)
            sample, originals, manifest = namespace['sealed_pair']()
            with tempfile.TemporaryDirectory(prefix='drill38-judge-') as temporary:
                namespace['evidence'](temporary, sample, originals, manifest)
                result = namespace['C'].judge(temporary, manifest)
                self.assertEqual(D.checked_judgment(result, ROWS['A-single'], manifest, originals), 0)

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_cleanup_requires_empty_and_no_remaining_identities(self):
        D.checked_empty(dict(empty=True, remaining=[]))
        for value in (dict(empty=False, remaining=[]), dict(empty=True, remaining=[dict(pid=10, start_ticks=50)]),
                      dict(empty=1, remaining=[])):
            with self.subTest(value=value), self.assertRaises(D.ContractError):
                D.checked_empty(value)

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_calibration_ack_cannot_replace_two_300_second_intervals(self):
        # Pure schedule control: a model clock proves the loop duration only.
        # It supplies no collector observations or product calibration receipt.
        now, observations = [0.0], []
        def sample():
            observations.append(now[0])
        def sleep(seconds):
            now[0] += seconds
        for _ in range(2):
            D.calibrate(sample, 700, clock=lambda:now[0], sleep=sleep)
        self.assertEqual(now[0], 600)
        self.assertGreaterEqual(len(observations), 120)
        with self.assertRaisesRegex(D.ContractError, 'calibration_does_not_fit_arm_deadline'):
            D.calibrate(sample, 650, clock=lambda:now[0], sleep=sleep)

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_envelope_cannot_weaken_profile_completion_or_case_expectation(self):
        with tempfile.TemporaryDirectory(prefix='drill38-envelope-') as directory:
            root = Path(directory)
            manifest, pair, doc = fixture_manifest(root)
            D.envelope(doc, ROWS['A-single'], F, SHARED)
            for field, value in (('phase', 'rejected'), ('native_rc', 64), ('completion', False), ('a6', False), ('hit_required', True)):
                changed = copy.deepcopy(manifest)
                changed['expect'][field] = value
                (root / 'fixture.json').write_text(json.dumps(changed))
                with self.subTest(field=field), self.assertRaises((D.ContractError, C.EvidenceError)):
                    D.envelope(doc, ROWS['A-single'], F, SHARED)
            (root / 'fixture.json').write_text(json.dumps(manifest))
            changed = copy.deepcopy(manifest)
            changed['actors'] = changed['actors'][:-1]
            (root / 'fixture.json').write_text(json.dumps(changed))
            with self.assertRaises(C.EvidenceError):
                D.envelope(doc, ROWS['A-single'], F, SHARED)
            (root / 'fixture.json').write_text(json.dumps(manifest))
            changed = dict(manifest, domains=manifest['domains'][:-1])
            (root / 'fixture.json').write_text(json.dumps(changed))
            with self.assertRaisesRegex(D.ContractError, 'fixture_domain_missing'):
                D.envelope(doc, ROWS['A-single'], F, SHARED)
            (root / 'fixture.json').write_text(json.dumps(manifest))
            for changed in (dict(manifest, source_commit=pair['a']['commit']),
                            dict(manifest, artifact_sha=pair['a']['binary_sha256'])):
                (root / 'fixture.json').write_text(json.dumps(changed))
                with self.assertRaisesRegex(D.ContractError, 'fixture_pair_mismatch'):
                    D.envelope(doc, ROWS['A-single'], F, SHARED)
            (root / 'fixture.json').write_text(json.dumps(manifest))
            for changed in (dict(doc, operations=doc['operations'][:-1]), dict(doc, mutation_patch_digest='a' * 64)):
                with self.assertRaises(D.ContractError):
                    D.envelope(changed, ROWS['A-single'], F, SHARED)
            broken = copy.deepcopy(pair)
            broken['b']['commit'] = broken['a']['commit']
            (root / 'pair.json').write_text(json.dumps(broken))
            with self.assertRaisesRegex(D.ContractError, 'distinct_s8_pair_required'):
                D.envelope(doc, ROWS['A-single'], F, SHARED)

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_every_command_requires_shared_supervision_and_hermetic_paths(self):
        with tempfile.TemporaryDirectory(prefix='drill38-command-') as directory:
            good = dict(argv=[SHARED, '--', '/usr/bin/true'], cwd=directory, timeout_s=5)
            D.command_spec(good, F.supervisor, SHARED)
            with self.assertRaises(D.ContractError):
                D.command_spec(dict(good, argv=['/usr/bin/true']), F.supervisor, SHARED)
            for value in ('relative.json', '/tmp/.tether/state', '/tmp/.local/bin/tether'):
                with self.assertRaises(D.ContractError):
                    D.safe_path(value)
            D.safe_path(directory + '/manifest.json')

    # origin: hot-update S8 plan §9.2 / §11.2 P27.
    def test_isolated_imports_and_shell_native_verdict_mapping(self):
        self.assertEqual(Path(F.HERE), ROOT / 'drills/lib')
        match = re.search(r'^upgrade38_native\(\) \{\n.*?^\}', SOURCE, re.M | re.S)
        self.assertIsNotNone(match)
        function = match.group()
        for rc in (0, 1, 2, 3, 4, 125):
            with self.subTest(rc=rc), tempfile.TemporaryDirectory(prefix='drill38-verdict-') as directory:
                shell = '. "$1/lib/assert.sh"\nlog() { :; }; ok() { :; }; err() { :; }\n'
                shell += 'ARM=R1\nINSTANCE=drill-contract-unit\nSIM_EVIDENCE_DIR="$2"\ndrill_begin drill38-hermetic\n'
                shell += function + '\nupgrade38_native "$3"\ndrill_end\n'
                result = subprocess.run([SHARED, '--', '/bin/sh', '-c', shell, 'drill38-contract', str(ROOT), directory, str(rc)],
                                        text=True, capture_output=True, timeout=15)
                expected = 2 if rc == 125 else rc
                self.assertEqual(result.returncode, expected, result.stderr)
                self.assertRegex(result.stderr, r'DRILL-VERDICT verdict=\S+ rc=' + str(expected) + ' ')



    # origin: hot-update S8 plan §8.3 / F8. Every arm binds an edit in a
    # real product function; the unmodified independent oracle is not patched.
    def test_mutation_catalog_binds_concrete_product_edits(self):
        faults = C.mutation_tools()
        for row in ROWS.values():
            if row['family'] != 'M':
                self.assertEqual(row['mutation'], 'none')
                continue
            with self.subTest(arm=row['arm']):
                recipe = faults.mutation_recipe(row)
                source = (REPO / recipe['file']).read_text()
                changed = faults.mutated_source(source, recipe)
                self.assertNotEqual(changed, source)
                self.assertEqual(recipe['oracle'], row['arm'] + ':')
                for bad in (dict(recipe,symbol='unrelatedSymbol'), dict(recipe,before='not an actual edit')):
                    with self.assertRaises(C.EvidenceError): faults.mutated_source(source,bad)
        rows = copy.deepcopy(ROWS)
        rows['M01']['mutation'] = 'none'
        with self.assertRaisesRegex(D.ContractError,'mutation_recipe_missing_or_unexpected'): scratch_contract(rows)

    # origin: hot-update S8 plan §8.3 / F8. This executes only the source
    # builder on a small archive. It is not a product-mutant kill receipt.
    def test_mutation_build_rejects_digest_only_wrong_symbol_and_unchanged_binary(self):
        faults = C.mutation_tools()
        row = ROWS['M19']
        recipe = faults.mutation_recipe(row)
        source = (REPO / recipe['file']).read_bytes()
        with tempfile.TemporaryDirectory(prefix='s8-build-control-') as temporary:
            root = Path(temporary)
            archive = root / 'source.tar'
            with tarfile.open(archive,'w') as stream:
                for name,raw in [('go.mod',b'module example.invalid/control\n'),(recipe['file'],source)]:
                    info=tarfile.TarInfo(name); info.size=len(raw); stream.addfile(info,io.BytesIO(raw))
            artifact=dict(source_archive=str(archive),archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
                          binary_sha256=hashlib.sha256(source).hexdigest(),commit='c'*40)
            record=faults.build_mutation(root/'built',row,artifact,'/unused/go',SHARED)
            path=root/'built/mutation-build.json'
            self.assertEqual(faults.validate_mutation_build(path,row),record)
            for defect in ('unchanged-source','wrong-symbol','fake-digest','unchanged-binary','other-file','schema','script-build','baseline-digest'):
                changed=copy.deepcopy(record)
                if defect=='unchanged-source': changed['after']=changed['before']
                elif defect=='wrong-symbol': changed['recipe']['symbol']='other'
                elif defect=='fake-digest': changed['patch_digest']='f'*64
                elif defect=='unchanged-binary': changed['artifact']['binary_sha256']=changed['binary_sha']
                elif defect=='schema': changed['claimed_pass']=True
                elif defect=='script-build': changed['command']=['/not/the/builder']
                elif defect=='baseline-digest': changed['baseline_sha']='f'*64
                else: (root/'built/source/extra.go').write_text('package extra')
                path.write_text(json.dumps(changed))
                with self.subTest(defect=defect),self.assertRaises(C.EvidenceError): faults.validate_mutation_build(path,row)
                if defect=='other-file': (root/'built/source/extra.go').unlink()
            path.write_text(json.dumps(record))
            self.assertEqual(faults.validate_mutation_build(path,row),record)
            with self.assertRaisesRegex(C.EvidenceError,'mutation_build_root_invalid'):
                faults.build_mutation(root/'built',row,artifact,'/unused/go',SHARED)
            owner={k:v for k,v in F.supervisor.identity(os.getpid()).items()}
            C.publish(root/'mutation-owner.json',owner)
            with self.assertRaisesRegex(C.EvidenceError,'mutation_script_not_executed'):
                faults.observe_mutation(root,dict(run_id='a'*32),row,path)
            read_bytes=Path.read_bytes
            def raced_cmdline(file):
                if file == Path('/proc')/str(owner['pid'])/'cmdline':
                    return ('/usr/bin/python3\0-I\0-B\0'+str(root/'built/source'/recipe['file'])+'\0').encode()
                return read_bytes(file)
            # Inject a PID-incarnation race at the second kernel read. This
            # is a failing syscall model, not a successful execution receipt.
            with mock.patch.object(Path,'read_bytes',raced_cmdline),mock.patch.object(faults.supervisor,'same_process',side_effect=[True,False]):
                with self.assertRaisesRegex(C.EvidenceError,'mutation_owner_changed'):
                    faults.observe_mutation(root,dict(run_id='a'*32),row,path)
            (root/'mutation-owner.json').write_text(json.dumps(dict(owner,pid=99999999)))
            with self.assertRaisesRegex(C.EvidenceError,'mutation_owner_not_live'):
                faults.observe_mutation(root,dict(run_id='a'*32),row,path)

    # origin: hot-update S8 plan §8.3 / F8. Archive bytes and recipe targets
    # are parsed before extraction; a supplied digest is never sufficient.
    def test_mutation_archive_and_recipe_are_closed(self):
        faults=C.mutation_tools()
        row=ROWS['M19']
        recipe=faults.mutation_recipe(row)
        for field,value in [('oracle','M01:'),('after',recipe['before']),('file','../../outside.py'),('build','arbitrary')]:
            changed=dict(recipe,**{field:value})
            with self.subTest(field=field),self.assertRaises(C.EvidenceError):
                faults.mutation_recipe(dict(row,mutation=json.dumps(changed)))
        with tempfile.TemporaryDirectory(prefix='s8-archive-controls-') as temporary:
            path=Path(temporary)/'source.tar'
            for defect in ('digest','root','path','link'):
                with tarfile.open(path,'w') as stream:
                    name='../escape' if defect=='path' else 'no-go.mod' if defect=='root' else 'go.mod'
                    info=tarfile.TarInfo(name)
                    if defect=='link': info.type=tarfile.SYMTYPE; info.linkname='/outside'
                    else: info.size=1
                    stream.addfile(info,None if defect=='link' else io.BytesIO(b'x'))
                sha='f'*64 if defect=='digest' else hashlib.sha256(path.read_bytes()).hexdigest()
                expected={'digest':'mutation_archive_changed','root':'mutation_archive_root_missing',
                          'path':'mutation_archive_path_invalid','link':'mutation_archive_member_invalid'}[defect]
                with self.subTest(defect=defect),self.assertRaisesRegex(C.EvidenceError,expected): faults.source_archive(path,sha)

    # origin: hot-update S8 plan §8.3 / F8. Build failures are exercised as
    # failures. The invalid native record below cannot sign a Go product run.
    def test_native_mutation_requires_compiler_success_and_running_bytes(self):
        faults=C.mutation_tools()
        row=ROWS['M01']; recipe=faults.mutation_recipe(row)
        source=(REPO/recipe['file']).read_bytes()
        with tempfile.TemporaryDirectory(prefix='s8-native-build-control-') as temporary:
            root=Path(temporary); archive=root/'source.tar'
            files={'go.mod':b'module example.invalid/control\n',recipe['file']:source}
            with tarfile.open(archive,'w') as stream:
                for name,raw in files.items():
                    info=tarfile.TarInfo(name); info.size=len(raw); stream.addfile(info,io.BytesIO(raw))
            artifact=dict(source_archive=str(archive),archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
                          binary_sha256='a'*64,commit='c'*40)
            with mock.patch.object(faults.supervisor,'run',return_value=dict(native_rc=1,stdout='compiler failure')):
                with self.assertRaisesRegex(C.EvidenceError,'mutation_build_failed'):
                    faults.build_mutation(root/'build',row,artifact,'/unused/go',SHARED)
            before=faults.source_inventory(files)
            files[recipe['file']]=faults.mutated_source(source.decode(),recipe).encode()
            after=faults.source_inventory(files)
            candidate=root/'build/candidate'; candidate.write_bytes(b'invalid compiler-failure artifact')
            baseline_candidate=root/'build/baseline-candidate'; baseline_candidate.write_bytes(b'invalid baseline compiler-failure artifact')
            process=dict(native_rc=1,stdout='compiler failure',executable_sha='b'*64,owner=F.supervisor.identity(os.getpid()))
            command=[SHARED,'--','/unused/go','-C',str(root/'build/source'),'build','-trimpath','-o',str(candidate),recipe['build']]
            record=dict(version=1,arm=row['arm'],recipe=recipe,artifact=artifact,before=before,after=after,
                        source_digest=C.digest(after),patch_digest=C.digest(dict(recipe=recipe,before=before[recipe['file']],after=after[recipe['file']])),
                        binary_sha=hashlib.sha256(candidate.read_bytes()).hexdigest(),command=command,process=process)
            baseline_command=list(command)
            baseline_command[4],baseline_command[8]=str(root/'build/baseline'),str(baseline_candidate)
            record.update(baseline_sha=hashlib.sha256(baseline_candidate.read_bytes()).hexdigest(),
                          baseline_command=baseline_command,baseline_process=process)
            path=root/'build/mutation-build.json'; path.write_text(json.dumps(record))
            with self.assertRaisesRegex(C.EvidenceError,'mutation_build_not_successful'): faults.validate_mutation_build(path,row)
            record['baseline_command'][6]='-race'; path.write_text(json.dumps(record))
            with self.assertRaisesRegex(C.EvidenceError,'mutation_baseline_flags_changed'): faults.validate_mutation_build(path,row)
            record['baseline_command'][6]='-trimpath'
            record['command'][5]='version'; path.write_text(json.dumps(record))
            with self.assertRaisesRegex(C.EvidenceError,'mutation_build_command_changed'): faults.validate_mutation_build(path,row)
            with self.assertRaisesRegex(C.EvidenceError,'mutation_not_in_compiled_output'):
                faults.require_compiled_change(record['binary_sha'],record['binary_sha'])
            execution=dict(executable_sha=hashlib.sha256(Path('/proc/self/exe').read_bytes()).hexdigest(),argv=['unmodified'])
            with self.assertRaisesRegex(C.EvidenceError,'mutation_binary_not_executed'):
                faults.validate_mutation_execution(record,execution,path)
            owner=F.supervisor.identity(os.getpid())
            execution['owner']=owner
            manifest=dict(artifact_sha=record['binary_sha'])
            payload=dict(Proof=dict(Observation=dict(census=dict(stamp=dict(producer=dict(owner,instance_id='unit-control'))))))
            C.mutation_owner_binding(record,execution,manifest,payload)
            for field in ('boot_id','pid','start_ticks'):
                foreign=copy.deepcopy(payload)
                foreign['Proof']['Observation']['census']['stamp']['producer'][field]='foreign' if field=='boot_id' else owner[field]+1
                with self.assertRaisesRegex(C.EvidenceError,'mutation_running_owner_foreign'):
                    C.mutation_owner_binding(record,execution,manifest,foreign)
            with self.assertRaisesRegex(C.EvidenceError,'mutation_running_owner_foreign'):
                C.mutation_owner_binding(record,execution,dict(artifact_sha='c'*64),payload)

    # origin: hot-update S8 plan §9.0 / F8. Product adapters supply raw files;
    # collector fixes the native command and refuses unrelated/missing inputs.
    def test_companion_producer_binds_actual_reader_input_and_native_failure(self):
        row=ROWS['R1']
        with tempfile.TemporaryDirectory(prefix='s8-companion-producer-') as temporary:
            root=Path(temporary)
            payload=dict(Seal=dict(Round=0),Round=0,Role='single',Point='R1')
            file='companion-data/p23/round-0/rollback/single-R1.json'
            (root/file).parent.mkdir(parents=True)
            C.publish(root/file,payload)
            reference=dict(path=file,sha256=hashlib.sha256((root/file).read_bytes()).hexdigest())
            C.companion_selection(row,[payload],[reference])
            with self.assertRaisesRegex(C.EvidenceError,'scenario_verifier_input_not_consumed'):
                C.companion_selection(row,[payload],[dict(reference,path='unread.json')])
            with self.assertRaisesRegex(C.EvidenceError,'scenario_selected_cell_foreign'):
                C.companion_selection(row,[dict(payload,Point='R0')],[reference])
            command=C.companion_command(root,row,SHARED,'/absolute/go')
            self.assertEqual(command,[SHARED,'--','/absolute/go','test','-json','-count=1','-tags=hotupdate_integration',
                                     '-timeout=90s','./test/hotupdate','-run','^'+row['test']+'$','-args',
                                     '-upgrade-transaction-evidence='+str(root/'companion-data')])
            manifest=dict(version=1,run_id='a'*32,arm='R1',source_commit='c'*40,artifact_sha='b'*64)
            request=dict(version=1,run_id=manifest['run_id'],arm='R1',original_set_digest='d'*64,
                         evidence_run_id='p23/round-0/rollback/single-R1',files=[reference])
            C.publish(root/'scenario-input.json',request)
            C.publish(root/'originals.json',dict(original_set_digest='d'*64))
            # Only a failure is injected. No fake verifier PASS can certify a
            # missing Go dependency or produce scenario-companion.json here.
            with mock.patch.object(C.supervisor,'call',return_value=dict(native_rc=1,stdout='native failure')) as called:
                with self.assertRaisesRegex(C.EvidenceError,'scenario_verifier_native_failure'):
                    C.produce_companion(root,manifest,row,SHARED,10)
                self.assertEqual(called.call_args.args[1]['spec']['argv'][3:5],['test','-json'])
            self.assertFalse((root/'scenario-companion.json').exists())
            self.assertEqual(C.read(root/'companion-invocation.json')['process']['native_rc'],1)
            with self.assertRaisesRegex(C.EvidenceError,'scenario_live_reader_missing'):
                C.companion_selection(ROWS['G-two'],[payload],[reference])
            with self.assertRaisesRegex(C.EvidenceError,'scenario_live_reader_missing'):
                C.companion_selection(ROWS['H-no-systemd'],[payload],[reference])
            with self.assertRaisesRegex(C.EvidenceError,'scenario_round_invalid'):
                C.companion_selection(row,[dict(payload,Round=5)],[reference])
            for arm in ('H-root','C-late-writer','C-gc','I-nats-cut-leader-move'):
                with self.subTest(arm=arm),self.assertRaisesRegex(C.EvidenceError,'scenario_selected_cell_foreign'):
                    C.companion_selection(ROWS[arm],[payload],[reference])
            with mock.patch.object(C.shutil,'which',return_value=None):
                with self.assertRaisesRegex(C.EvidenceError,'scenario_verifier_unavailable'):
                    C.produce_companion(root,manifest,row,SHARED,10)
            C.publish(root/'scenario-input-foreign.json',dict(request,run_id='b'*32))
            (root/'scenario-input.json').write_bytes((root/'scenario-input-foreign.json').read_bytes())
            with self.assertRaisesRegex(C.EvidenceError,'scenario_input_foreign'):
                C.produce_companion(root,manifest,row,SHARED,10)
            (root/'scenario-input.json').write_text(json.dumps(request))
            def changed_input(*args):
                (root/file).write_text(json.dumps(dict(payload,Point='R0')))
                return dict(native_rc=1,stdout='native failure')
            (root/'companion-invocation.json').unlink()
            with mock.patch.object(C.supervisor,'call',side_effect=changed_input):
                with self.assertRaisesRegex(C.EvidenceError,'scenario_verifier_inputs_changed'):
                    C.produce_companion(root,manifest,row,SHARED,10)
        with tempfile.TemporaryDirectory(prefix='s8-empty-corpus-') as temporary:
            with self.assertRaisesRegex(C.EvidenceError,'scenario_verifier_corpus_missing'): C.companion_corpus(Path(temporary))

    # origin: hot-update S8 plan §8.3 / F8. A native assertion from any earlier
    # observer/command must remain unverified until the exact M oracle passes.
    def test_unrelated_assert_red_cannot_sign_a_killed_mutant(self):
        self.assertEqual(D.mutation_native(ROWS['M01'],[1],False),4)
        self.assertEqual(D.mutation_native(ROWS['M01'],[1,2],False),2)
        self.assertEqual(D.mutation_native(ROWS['M01'],[1,3],False),4)
        self.assertEqual(D.mutation_native(ROWS['M01'],[1],True),1)
        self.assertEqual(D.mutation_native(ROWS['R1'],[1],False),1)
        self.assertIn('C.produce_companion(root, manifest, row,',SOURCE)
        self.assertIn('faults.observe_mutation(root, manifest, row,',SOURCE)
        probe=SOURCE.index("            if row['family'] == 'M' and (root / 'mutation-owner.json').is_file()")
        self.assertLess(probe,SOURCE.index("            sample = observed(lambda:"))
        self.assertIn('native = mutation_native(row, statuses, mutant_verified)',SOURCE)
        with tempfile.TemporaryDirectory(prefix='s8-runner-failure-') as temporary:
            with mock.patch.object(F,'start',side_effect=IndexError('unclassified fixture failure')):
                receipt=D.execute_arm(ROWS['M01'],dict(deadline_s=10,source_commit='a'*40,artifact_sha='b'*64,run_id='c'*32),
                                      dict(mutation_patch_digest='d'*64),temporary,F)
            self.assertEqual(receipt['native_rc'],2)
            self.assertFalse(receipt['mutant_verified'])


def mutation_checks():
    # origin: hot-update S8 plan §11.2 P27. Mutate the actual scanner inputs,
    # run a fresh supervised selftest process, and restore even after failure.
    original = SCRIPT.read_text()
    receipts = Path(tempfile.mkdtemp(prefix='drill38-mutation-receipts-', dir=os.environ.get('S8_RECEIPTS_DIR')))
    def omitted_guard(reason):
        marker = original.index("'" + reason + "'")
        begin = original.rfind('need(', 0, marker)
        end = original.index(')\n', marker) + 1
        return original[begin:end], 'need(True, ' + original[marker:end]
    guards = [
        ('r1-preflight-as-partial', 'r1_partial_take_not_hit'),
        ('reverse-cut-reused', 'reverse_reused_forward_attempt'),
        ('forward-receipts-reused', 'forward_receipt_reused'),
        ('empty-census-forged', 'fixture_not_empty_after_stop'),
        ('shared-supervisor-bypassed', 'shared_supervisor_required'),
        ('completion-weakened', 'fixture_completion_weakened'),
        ('a6-oracle-disabled', 'fixture_required_oracle_changed'),
        ('r2-no-new-admission', 'r2_new_request_not_observed'),
        ('verifier-rc-laundered', 'scenario_judge_rc_laundered'),
        ('verifier-scope-replayed', 'scenario_judge_stale'),
        ('mutant-green-accepted', 'mutant_did_not_turn_red'),
        ('same-source-pair-accepted', 'distinct_s8_pair_required'),
        ('cross-artifact-provenance-accepted', 'fixture_pair_mismatch'),
        ('r0-new-owner-accepted', 'r0_original_owner_changed'),
        ('production-path-accepted', 'production_or_relative_path_forbidden'),
        ('calibration-deadline-ignored', 'calibration_does_not_fit_arm_deadline'),
        ('revocation-loses-to-rollback', 'revocation_priority_weakened'),
        ('readonly-skips-rollback', 'readonly_reverse_step_missing'),
        ('full-profile-domain-missing', 'fixture_domain_missing'),
        ('mutation-recipe-erased', 'mutation_recipe_missing_or_unexpected'),
    ]
    mutations = [(name, *omitted_guard(reason)) for name, reason in guards]
    mutations += [
        ('new-admission-as-partial-take', "resources = [r for r in sample['resources'] if r['id'] in original]", "resources = sample['resources']"),
        ('receipt-native-status-lost', "return result['native_rc']", "return result['rc']"),
        ('child-rc-laundered', 'return 0 if child_rc == expected_rc else 3', 'return 0'),
        ('header-budget-not-checked', 'need(actual == [expected],', 'need(True,'),
        ('case-expectation-unbound', "need(row[local] == case[upstream],", 'need(True,'),
        ('shell-assert-red-green', '1) _as_fail "38.$ARM native ASSERT-RED"', '1) _as_pass "38.$ARM native ASSERT-RED"'),
        ('shell-incomplete-green', '4) not_covered "38.$ARM native INCOMPLETE" "required live interval/proof was not observed" gap',
                                  '4) _as_pass "38.$ARM native INCOMPLETE"'),
        ('dispatch-child-missing', '    R1) upgrade38_run ;;', ''),
        ('reverse-generation-gap', "final['generation'] == checkpoint['generation'] + 1", "final['generation'] > checkpoint['generation']"),
        ('calibration-marker-only', 'until = clock() + 300', 'until = clock()'),
        ('unrelated-assert-signs-mutant', "if row['family'] == 'M' and not verified:", 'if False:'),
        ('companion-producer-disconnected', 'C.produce_companion(root, manifest, row,', 'C.missing_companion(root, manifest, row,'),
        ('mutation-running-probe-disconnected', "            if row['family'] == 'M' and (root / 'mutation-owner.json').is_file() and not (root / 'mutation-execution.json').exists():", '            if False:'),
        ('runner-exception-counts-as-mutant', '    except Exception:\n        # Unclassified runner failures',
                                            '    except (OSError, ValueError, KeyError, TypeError):\n        # Unclassified runner failures'),
    ]
    results = []
    try:
        for name, needle, replacement in mutations:
            if original.count(needle) != 1:
                raise AssertionError('mutation anchor is not unique: ' + name)
            changed = original.replace(needle, replacement, 1)
            patch = ''.join(difflib.unified_diff(original.splitlines(True), changed.splitlines(True),
                                               fromfile=str(SCRIPT), tofile=str(SCRIPT)))
            SCRIPT.write_text(changed)
            try:
                result = subprocess.run([SHARED, '--', '/bin/sh', str(ROOT / 'tests/drill38-contract-test.sh')],
                                        text=True, capture_output=True, timeout=30)
                (receipts / (name + '.patch')).write_text(patch)
                (receipts / (name + '.log')).write_text(result.stdout + result.stderr)
                row = dict(mutation=name, native_rc=result.returncode,
                           patch_digest=hashlib.sha256(patch.encode()).hexdigest())
                results.append(row)
                print(json.dumps(row), flush=True)
                if result.returncode != 1:
                    raise AssertionError('mutation did not produce native assertion red: ' + name)
            finally:
                SCRIPT.write_text(original)
    finally:
        SCRIPT.write_text(original)
        (receipts / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
        print('Mutation receipts: ' + str(receipts), flush=True)
    result = subprocess.run([SHARED, '--', '/bin/sh', str(ROOT / 'tests/drill38-contract-test.sh')],
                            text=True, capture_output=True, timeout=30)
    (receipts / 'restored-positive.log').write_text(result.stdout + result.stderr)
    print(json.dumps(dict(restored_positive_rc=result.returncode, mutants=len(results))), flush=True)
    if result.returncode != 0:
        raise AssertionError('restored positive did not pass')


if sys.argv[1:] == ['--mutations']:
    mutation_checks()
else:
    unittest.main(argv=['drill38-contract', *sys.argv[1:]], verbosity=2)
PY38TEST
exit "$S8_TEST_RC"
