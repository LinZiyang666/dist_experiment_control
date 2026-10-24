#!/usr/bin/env -S python3 -I -B
"""Hermetic mutation-manifest controls; no product or deployment receipts.

Synthetic archives below exercise rejection only. No process, network, Docker
or installed tether is launched, and no independent leak evidence is invented.
"""
import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import re
import runpy
import sys
import tempfile
import types
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('fullstack_mutations', HERE / 'fullstack_mutations.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


def ref(root, name, raw):
    path = root / name
    path.write_bytes(raw)
    return dict(path=name, sha=M.sha(raw), bytes=len(raw))


def json_ref(root, name, value):
    return ref(root, name, M.canonical(value))


# origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
class FullstackMutationManifestTest(unittest.TestCase):
    # origin: hot-update S10 plan §8 / §10.2 P20
    def setUp(self):
        self.catalog = M.manifest()
        self.row = next(r for r in self.catalog['mutations'] if r['id'] == 'M03F')

    def reject(self, fn, *args, contains=None):
        with self.assertRaises(M.ContractError) as error:
            fn(*args)
        if contains:
            self.assertIn(contains, str(error.exception))

    def patch(self, row=None):
        row = row or self.row
        source = (M.REPO / row['source_path']).read_bytes()
        predicate = row['predicate']
        if row['surface'] == 'harness':
            before = predicate
            after = "None if request.slot.id == 'synthetic/slot/00000001' else self.launch(request)"
        else:
            before, after = predicate, predicate.replace('uint64(n)', 'uint64(n) + 1')
        return dict(schema='fullstack-mutation-patch-v1', mutation_id=row['id'], source_sha=M.sha(source),
                    before=before, after=after)

    def fixture(self, root):
        # These artifact bytes are never executed. A passing parser test is
        # deliberately separate from the native live 0->1->0 product claim.
        patch = self.patch()
        M.archive(root, 'patch.json', patch)
        (root / 'sealed-schedule.json').write_bytes(b'presealed synthetic slots')
        source = M.prepare_patch(self.row, patch, M.REPO, root / 'mutation', '-race')
        for phase in M.PHASES:
            directory = root / phase
            directory.mkdir()
            rc = 1 if phase == 'mutant' else 0
            hit = dict(mutation_id=self.row['id'], role=self.row['role'], window=self.row['window'],
                       event=self.row['hit_event'], cut_id='synthetic-cut', generation=2, pid=123, start_ticks=456,
                       boot_id='synthetic-boot', start_ns=10, at_ns=20, end_ns=40, assertion_ns=30, pending=1, accepted=True)
            builds = []
            for kind in ('outer', 'product', 'nested'):
                raw = ('synthetic parser bytes; never executed: ' + kind + ('mutant' if rc and kind != 'outer' else 'baseline')).encode()
                builds.append(dict(kind=kind, role=self.row['role'], goflags=source['goflags'] if rc else '-race',
                                   overlay_digest=source['overlay_digest'] if rc else '',
                                   source_sha=source['mutant_source_sha'] if rc else source['source_sha'],
                                   artifact=ref(directory, kind + '.artifact', raw),
                                   executing=ref(directory, kind + '.executing', raw)))
            receipt = dict(schema='fullstack-mutation-run-v1', mutation_id=self.row['id'], phase=phase,
                           surface='product', topology='single', tier='hermetic', artifact_set=self.row['compatible_artifact_set'],
                           source_sha=source['source_sha'], dirty_digest=M.sha(b'synthetic candidate'),
                           patch_digest=source['patch_digest'], manifest_digest=M.sha(M.canonical(self.catalog)),
                           schedule=ref(directory, 'schedule.json', b'presealed synthetic slots'),
                           oracle_digest=M.oracle_digest(self.row), reader_digest=M.sha(b'synthetic parser reader'),
                           native_rc=rc, verdict=M.VERDICTS[rc], assertion=self.row['expected_assertion'] if rc else '',
                           hit=json_ref(directory, 'hit.json', hit), builds=builds,
                           raw=ref(directory, 'native.log', ('synthetic parser control ' + (self.row['expected_assertion'] if rc else 'GREEN')).encode()),
                           supervision=json_ref(directory, 'supervision.json', dict(native_rc=rc, joined=True, remaining=[])))
            M.archive(directory, 'waiter.json', {'native_rc': rc})
            M.archive(directory, 'receipt.json', receipt)
        return source

    def edit_receipt(self, root, edit, phase='mutant'):
        path = root / phase / 'receipt.json'
        value = M.load(path)
        edit(value)
        path.write_bytes(M.canonical(value))

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_all_fifty_bind_to_actual_source_and_existing_integration_anchors(self):
        self.assertEqual(50, len(self.catalog['mutations']))
        self.assertEqual(list(M.KEYS), [r['id'] for r in self.catalog['mutations']])
        self.assertEqual({'harness'}, {r['surface'] for r in self.catalog['mutations'] if r['id'].startswith('M10')})
        self.assertTrue(all(r['deploy_only'] for r in self.catalog['mutations'] if r['id'] in ('M07N', 'M07C', 'M08C', 'M11C')))
        for key in ('M01B', 'M06F', 'M07N', 'M07C', 'M11D', 'M11L', 'M11F'):
            row = next(r for r in self.catalog['mutations'] if r['id'] == key)
            self.assertEqual(['single', 'quorum'], row['topologies'])

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_missing_replaced_duplicate_and_empty_source_bindings_are_red(self):
        for change in ('drop', 'duplicate', 'count-only', 'wrong-source', 'wrong-function', 'wrong-predicate', 'test-only', 'fake-anchor'):
            with self.subTest(change=change):
                bad = copy.deepcopy(self.catalog)
                if change == 'drop': bad['mutations'].pop()
                if change == 'duplicate': bad['mutations'][-1] = bad['mutations'][0]
                if change == 'count-only': bad['required_children'][-1] = 'M11Z'
                if change == 'wrong-source': bad['mutations'][0]['source_path'] = 'internal/absent.go'
                if change == 'wrong-function': bad['mutations'][0]['function'] = 'Absent'
                if change == 'wrong-predicate': bad['mutations'][0]['predicate'] = 'absentPredicate()'
                if change == 'test-only': bad['mutations'][0]['source_path'] = 'test/hotupdate/fullstack_task_exec_test.go'
                if change == 'fake-anchor': bad['mutations'][0]['integration']['exercise'] = 'fakeExercise'
                self.reject(M.validate_manifest, bad)

    # Each row's own symbol is removed on disk in a private copy of the bound
    # files: renamed function, predicate only in a comment, predicate only in a
    # literal. The substring-only check kept the comment and literal variants
    # "bound"; a predicate that also moves a brace may shift the body instead.
    # origin: hot-update S10 plan §8 / §10.2 P20; r9d REPORT Request 4
    def test_every_row_binds_live_code_and_a_vanished_symbol_is_red(self):
        rows = self.catalog['mutations']
        files = {p for r in rows for p in (r['source_path'], r['integration']['path'], r['integration']['oracle_path'])}
        prose = 'predicate survives only in a comment or literal'
        unbound = {prose, 'predicate absent from actual function', 'unterminated Go source binding'}
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            for relative in files:
                (repo / relative).parent.mkdir(parents=True, exist_ok=True)
                (repo / relative).write_bytes((M.REPO / relative).read_bytes())
            self.assertIs(self.catalog, M.validate_manifest(self.catalog, repo))
            for row in rows:
                path = repo / row['source_path']
                original = path.read_text()
                body = M.function_body(original, row['function'], path.suffix)
                self.assertTrue(M.binds_live_code(body, row['predicate'], path.suffix), row['id'])
                name, predicate, python = row['function'].split('.')[-1], row['predicate'], path.suffix == '.py'
                balanced = predicate.count('{') == predicate.count('}')
                # The declaration is the first `name(` of the extracted body.
                renamed = re.sub(r'\b' + re.escape(name) + r'(\s*\()', name + r'Vanished\1', body, count=1)
                variants = {'renamed': renamed,
                            'comment': body.replace(predicate, 'None  # ' + predicate if python else 'nil /* ' + predicate + ' */'),
                            'literal': body.replace(predicate, repr(predicate) if python else '`' + predicate + '`')}
                for variant, changed in variants.items():
                    with self.subTest(row=row['id'], variant=variant):
                        self.assertNotEqual(body, changed)
                        path.write_text(original.replace(body, changed, 1))
                        try:
                            with self.assertRaises(M.ContractError) as error:
                                M.check_binding(row, repo)
                        finally:
                            path.write_text(original)
                        expected = ({'missing or ambiguous ' + ('Python' if python else 'Go') + ' source binding'}
                                    if variant == 'renamed' else {prose} if balanced else unbound)
                        self.assertIn(str(error.exception), expected)
            self.assertIs(self.catalog, M.validate_manifest(self.catalog, repo))
        # A truncated Python fragment must not degrade to its matching prefix.
        self.assertTrue(M.binds_live_code('def f():\n    g(1)\n', 'g(1)', '.py'))
        self.reject(M.binds_live_code, 'def f():\n    g(1)\n', 'g(', '.py', contains='unparsable Python predicate')

    # S8 deleted the install call M06F was bound to and moved the executing-image
    # hash M09A reads; their stale rows turned all 50 drill 100 arms SETUP-RED.
    # M06F now binds the fleet census every participant feeds (the decision
    # that refuses a third release on a non-voter or agent; its transaction
    # caller is still S8 P17 runtime-preflight wiring, so the live kill stays
    # NOT_RUN with its listed owners); M09A binds the /proc/self/exe selection
    # inside the moved RunningImageSHA.
    # origin: hot-update S10 plan §8 M06F/M09A; r9d REPORT Request 4
    def test_rows_moved_by_s8_bind_their_landed_decision_points(self):
        rows = {r['id']: r for r in self.catalog['mutations']}
        for key, decision in (('M06F', 'deferUpgrade("fleet_third_release")'), ('M09A', 'agentimage.SHA256OfFile(path)')):
            row = rows[key]
            body = M.function_body((M.REPO / row['source_path']).read_text(), row['function'], '.go')
            self.assertTrue(M.binds_live_code(body, decision, '.go'), key + ' predicate is away from its decision')
        stale = {'M06F': ('internal/agent/upgrade.go', 'handleUpgradeInstallLocked',
                          'a.installNewBinary(body, exePath, installPolicy{readers: &readers, runningSHA: runningSHA})'),
                 'M09A': ('internal/agent/upgrade_state.go', 'runningImageSHA', 'p = "/proc/self/exe"')}
        for key, (source, function, predicate) in stale.items():
            with self.subTest(key=key):
                bad = copy.deepcopy(self.catalog)
                next(r for r in bad['mutations'] if r['id'] == key).update(source_path=source, function=function, predicate=predicate)
                self.reject(M.validate_manifest, bad, contains=key + ': predicate absent from actual function')

    # Drill 100 maps native 1 to the expected ASSERT-FAIL kill, and Python's
    # default status for an uncaught exception is also 1. The real __main__
    # statement runs in-process (no child): a crash is SETUP 2, while the
    # module's own chain result (1) and Pending (4) are left untouched.
    # origin: hot-update S10 plan §8 / L9; r9d REPORT Request 4
    def test_unclassified_driver_crash_exits_setup_never_the_kill_status(self):
        def entry(*argv):
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(io.StringIO()), \
                    mock.patch.object(sys, 'argv', ['fullstack_mutations.py', *argv]):
                with self.assertRaises(SystemExit) as status:
                    runpy.run_path(str(HERE / 'fullstack_mutations.py'), run_name='__main__')
            return status.exception.code, stderr.getvalue()
        with tempfile.TemporaryDirectory() as temporary:
            good, deep = Path(temporary) / 'good', Path(temporary) / 'deep'
            good.mkdir()
            self.fixture(good)
            check = ('check', '--mutation', self.row['id'], '--topology', 'single', '--root')
            self.assertEqual(1, entry(*check, str(good))[0])
            (deep / 'mutation').mkdir(parents=True)
            (deep / 'mutation/source.json').write_bytes(b'[' * 200000 + b']' * 200000)
            code, stderr = entry(*check, str(deep))
            self.assertEqual((2, True), (code, 'unclassified driver failure: RecursionError' in stderr), stderr)
            code, stderr = entry(*check, str(Path(temporary) / 'absent'))
            self.assertEqual((2, True), (code, 'invalid control binding or receipt' in stderr), stderr)
            (Path(temporary) / 'children.json').write_text('[]')
            code, stderr = entry('join', '--children', str(Path(temporary) / 'children.json'))
            self.assertEqual((4, True), (code, 'NOT_RUN pending P19/P25 mutation cells' in stderr), stderr)

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_manifest_does_not_alias_required_catalog(self):
        value = M.manifest()
        value['required_children'].pop()
        self.assertEqual(50, len(M.manifest()['required_children']))

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_go_binding_ignores_comments_strings_and_channel_signature_braces(self):
        source = 'package sample\nfunc Run(stop <-chan struct{}) {\n // } fake\n s := "}"\n go pump(stop)\n}\n'
        body = M.function_body(source, 'Run', '.go')
        self.assertIn('go pump(stop)', body)
        self.reject(M.function_body, source + source, 'Run', '.go')
        self.reject(M.function_body, source, 'Missing', '.go')
        self.assertEqual(M.tokens('pump() // ignored', '.go'), M.tokens('pump()', '.go'))
        self.assertNotEqual(M.tokens('url("//a")', '.go'), M.tokens('url("//b")', '.go'))
        self.reject(M.function_body, 'package sample\nconst bait = `\nfunc Ghost() { fake() }\n`\n', 'Ghost', '.go')
        self.reject(M.function_body, 'package sample\n/*\nfunc Ghost() { fake() }\n*/\n', 'Ghost', '.go')

    # origin: hot-update S10 plan §8 / §12.3 / FullstackMutationManifestTest
    def test_binding_cache_observes_on_disk_mutations_at_the_same_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'runtime.go'
            path.write_text('package sample\nfunc Run() { pump() }\n')
            original = M.function_body(path.read_text(), 'Run', '.go')
            path.write_text('package sample\nfunc Run() { lose() }\n')
            self.assertIn('lose()', M.function_body(path.read_text(), 'Run', '.go'))
            self.assertNotEqual(original, M.function_body(path.read_text(), 'Run', '.go'))

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_duplicate_nonfinite_json_and_escaped_raw_are_rejected(self):
        for raw in ('{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}'):
            self.reject(M.decode, raw)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'raw').write_text('retained')
            for path in ('../raw', '/raw', '', 'absent'):
                self.reject(M.file_at, root, path)
            (root / 'alias').symlink_to(root / 'raw')
            self.reject(M.file_at, root, 'alias')
            self.reject(M.read_ref, root, dict(path='raw', sha=M.sha(b'other'), bytes=8))

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_receipt_paths_cannot_select_production_state_or_installed_tether(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            # Artificial private paths only; production state is never opened.
            for name in ('.tether/state.json', '.local/bin/tether'):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{}')
                self.reject(M.load, path, contains='production path forbidden')
                self.reject(M.file_at, root, name, contains='production path forbidden')

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_overlay_preserves_inherited_flags_without_touching_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prior = root / 'prior.json'
            prior.write_bytes(M.canonical({'Replace': {'/sandbox/original.go': '/sandbox/prior.go'}}))
            original = (M.REPO / self.row['source_path']).read_bytes()
            result = M.prepare_patch(self.row, self.patch(), M.REPO, root / 'mutation', '-race -tags=hotupdate_integration -overlay=' + str(prior))
            flags = result['goflags']
            self.assertIn('-race', flags)
            self.assertIn('-tags=hotupdate_integration', flags)
            self.assertEqual(1, sum(f.startswith('-overlay=') for f in M.shlex.split(flags)))
            replacements = M.load(root / 'mutation/overlay.json')['Replace']
            self.assertEqual('/sandbox/prior.go', replacements['/sandbox/original.go'])
            self.assertEqual(original, (M.REPO / self.row['source_path']).read_bytes())
            self.assertIn('uint64(n) + 1', (root / 'mutation/source.after.go').read_text())

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_comment_only_stale_wrong_function_or_expected_patch_is_red(self):
        for change in ('comment', 'same', 'stale', 'wrong-function', 'wrong-mutation', 'unknown-field'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                patch = self.patch()
                if change == 'comment': patch['after'] = patch['before'] + ' // ignored'
                if change == 'same': patch['after'] = patch['before']
                if change == 'stale': patch['source_sha'] = '0' * 64
                if change == 'wrong-function': patch['before'] = 'return nil'
                if change == 'wrong-mutation': patch['mutation_id'] = 'M03E'
                if change == 'unknown-field': patch['expected'] = 0
                self.reject(M.prepare_patch, self.row, patch, M.REPO, Path(temporary) / 'mutation')

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_scheduler_mutates_actual_launch_or_probe_and_preserves_expected(self):
        for key in ('M10S',):
            row = next(r for r in self.catalog['mutations'] if r['id'] == key)
            with self.subTest(key=key), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                before = (M.REPO / row['source_path']).read_bytes()
                result = M.prepare_patch(row, self.patch(row), M.REPO, root / 'mutation')
                self.assertEqual('', result['overlay_digest'])
                changed = Path(result['harness_root'], Path(row['source_path']).name).read_text()
                body = M.function_body(changed, row['function'], '.py')
                self.assertIn("request.slot.id == 'synthetic/slot/00000001'", body)
                self.assertEqual(before, (M.REPO / row['source_path']).read_bytes())
                bad = self.patch(row)
                bad['after'] = 'self.schedule.slots.pop()'
                self.reject(M.prepare_patch, row, bad, M.REPO, root / 'wrong')

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_scheduler_control_really_omits_exactly_one_launch(self):
        row = next(r for r in self.catalog['mutations'] if r['id'] == 'M10S')
        with tempfile.TemporaryDirectory() as temporary:
            source = M.prepare_patch(row, self.patch(row), M.REPO, Path(temporary) / 'mutation')
            path = Path(source['harness_root'], 'fullstack_requests.py')
            module = types.ModuleType('fullstack_mutated_scheduler_control')
            sys.modules[module.__name__] = module
            try:
                exec(compile(path.read_text(), str(path), 'exec'), module.__dict__)
                key = module.Key('agent', 'host', 'unit', 'sid', 'nid', 'domain', 1000)
                slots = tuple(module.Slot('synthetic/slot/%08d' % i, 'read', key, i, i + 5_000_000_000) for i in range(4))
                schedule = module.Schedule(slots, M.sha(b'presealed'), b'presealed')
                calls = []
                scheduler = module.Scheduler(schedule, lambda request: calls.append(request.slot.id) or object(), clock=lambda: 10)
                scheduler.tick()
                self.assertEqual([slots[i].id for i in (0, 2, 3)], calls)
                self.assertEqual(4, len(schedule.slots))
                self.assertEqual(4, len(scheduler.report.launches))
                self.assertEqual('launch-error', scheduler.report.launches[1].error)
            finally:
                del sys.modules[module.__name__]

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_targeted_probe_sender_cannot_be_faked_from_before_step(self):
        row = next(r for r in self.catalog['mutations'] if r['id'] == 'M10C')
        with tempfile.TemporaryDirectory() as temporary:
            patch = self.patch(row)
            patch['after'] = 'None'
            with self.assertRaises(M.Pending) as error:
                M.prepare_patch(row, patch, M.REPO, Path(temporary) / 'mutation')
            self.assertIn('P19/P25 exact targeted-cut probe sender', str(error.exception))

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_synthetic_archive_validates_without_claiming_real_product(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            result = M.check_triplet(self.row, root, 'single', M.sha(M.canonical(self.catalog)))
            self.assertEqual([0, 1, 0], result['chain'])
            self.assertEqual(1, result['native_rc'])
            self.assertEqual(3, len(result['receipts']))

    # Mandatory P20 metamutation: dropping only the nested builder's GOFLAGS
    # must fail even when the outer test and product build report the overlay.
    # origin: hot-update S10 plan §10.2 P20 / overlay-not-entering-nested-build
    def test_overlay_must_enter_the_nested_build(self):
        for defect in ('flags', 'digest', 'source', 'absent', 'executing'):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.fixture(root)
                def edit(value):
                    build = next(b for b in value['builds'] if b['kind'] == 'nested')
                    if defect == 'flags': build['goflags'] = '-race'
                    if defect == 'digest': build['overlay_digest'] = 'f' * 64
                    if defect == 'source': build['source_sha'] = 'f' * 64
                    if defect == 'absent': value['builds'].remove(build)
                    if defect == 'executing': build['executing'] = ref(root / 'mutant', 'wrong.executing', b'unmutated')
                self.edit_receipt(root, edit)
                self.reject(M.check_triplet, self.row, root, 'single')

    # origin: hot-update S10 plan §10.2 P20 / red-in-wrong-window
    def test_red_must_hit_the_required_nonempty_window_and_domain(self):
        for field, value in [('window', 'after-teardown'), ('role', 'ctl'), ('event', 'wrong-hit'), ('pending', 0),
                             ('accepted', False), ('at_ns', 41), ('assertion_ns', 19), ('generation', 0),
                             ('pid', 0), ('start_ticks', 0), ('boot_id', ''), ('cut_id', '')]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.fixture(root)
                def edit(receipt):
                    hit = M.decode(M.read_ref(root / 'mutant', receipt['hit']))
                    hit[field] = value
                    receipt['hit'] = json_ref(root / 'mutant', 'wrong-hit.json', hit)
                self.edit_receipt(root, edit)
                self.reject(M.check_triplet, self.row, root, 'single')

    # origin: hot-update S10 plan §10.2 P20 / only-changing-expected
    def test_only_changing_expected_cannot_sign_a_mutation(self):
        for change in ('source', 'artifact', 'schedule', 'oracle', 'assertion', 'reader', 'manifest'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.fixture(root)
                if change == 'source':
                    original = (root / 'mutation/source.before').read_bytes()
                    (root / 'mutation/source.after.go').write_bytes(original)
                    path = root / 'mutation/source.json'
                    source = M.load(path)
                    source['mutant_source_sha'] = M.sha(original)
                    path.write_bytes(M.canonical(source))
                else:
                    def edit(receipt):
                        if change == 'artifact':
                            for build in receipt['builds']:
                                raw = ('synthetic parser bytes; never executed: ' + build['kind'] + 'baseline').encode()
                                build['artifact'] = ref(root / 'mutant', build['kind'] + '.artifact', raw)
                                build['executing'] = ref(root / 'mutant', build['kind'] + '.executing', raw)
                        if change == 'schedule': receipt['schedule'] = ref(root / 'mutant', 'changed-schedule', b'only surviving slots')
                        if change == 'oracle': receipt['oracle_digest'] = 'e' * 64
                        if change == 'assertion': receipt['assertion'] = 'another-domain'
                        if change == 'reader': receipt['reader_digest'] = 'e' * 64
                        if change == 'manifest': receipt['manifest_digest'] = 'e' * 64
                    self.edit_receipt(root, edit)
                self.reject(M.check_triplet, self.row, root, 'single')

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_compile_setup_refusal_timeout_and_native_translation_are_not_kills(self):
        for code in (0, 2, 3, 4, 64, 70, 75, 125, -9):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.fixture(root)
                (root / 'mutant/waiter.json').write_bytes(M.canonical({'native_rc': code}))
                self.reject(M.check_triplet, self.row, root, 'single')
        for message in ('NOT_RUN pending S8', 'SETUP-RED', 'build failed', 'panic: test timed out'):
            with self.subTest(message=message), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.fixture(root)
                self.edit_receipt(root, lambda r: r.update(raw=ref(root / 'mutant', 'setup.log',
                                   (message + ' ' + self.row['expected_assertion']).encode())))
                self.reject(M.check_triplet, self.row, root, 'single')

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_supervisor_cleanup_poison_and_unjoined_workers_are_red(self):
        for joined, remaining, code in ((False, [], 1), (True, [123], 1), (True, [], 125)):
            with self.subTest(code=code, joined=joined), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.fixture(root)
                self.edit_receipt(root, lambda r: r.update(supervision=json_ref(root / 'mutant', 'bad-supervision.json',
                                   dict(native_rc=code, joined=joined, remaining=remaining))))
                self.reject(M.check_triplet, self.row, root, 'single')

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_restore_must_use_original_artifact_source_and_oracle(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            self.edit_receipt(root, lambda r: r['builds'][0].update(source_sha='a' * 64), 'restore')
            self.reject(M.check_triplet, self.row, root, 'single')

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_same_resealed_expected_in_all_three_phases_is_red(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            for phase in M.PHASES:
                self.edit_receipt(root, lambda r: r.update(schedule=ref(root / phase, 'replaced-schedule', b'only surviving slots')), phase)
            self.reject(M.check_triplet, self.row, root, 'single', contains='independent expected schedule')

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_parent_requires_original_waiter_join_capability(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            child = dict(mutation_id=self.row['id'], topology='single', root=str(root))
            with self.assertRaises(M.Pending) as error:
                M.join_children([child], self.catalog)
            self.assertIn('P32 original mutation child waiter', str(error.exception))

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_managed_infra_cannot_be_signed_by_hermetic_parser_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.fixture(root)
            row = copy.deepcopy(self.row)
            row['deploy_only'] = True
            self.reject(M.check_phase, row, M.load(root / 'mutant/receipt.json'), source,
                        'mutant', 'single', root / 'mutant', 1, contains='managed infra')

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_missing_cells_remain_not_run_and_never_skip_or_pass(self):
        with self.assertRaises(M.Pending) as error:
            M.join_children([], self.catalog)
        self.assertIn('NOT_RUN pending P19/P25', str(error.exception))
        for row in self.catalog['mutations']:
            for cell in row['topologies']:
                self.assertIn(row['id'] + '/' + cell, str(error.exception))

    # origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
    def test_shell_exact_arms_and_five_metadata_maps_match_manifest(self):
        source = (HERE.parent / '100-hot-update-fullstack-controls.sh').read_text()
        for name in ('arms', 'fixture', 'grows', 'worst', 'forgoes'):
            header = next(x.split(': ', 1)[1] for x in source.splitlines() if x.startswith('# ' + name + ': '))
            keys = [x.split('=')[0] for x in header.split()]
            self.assertEqual(list(M.KEYS), keys)
        self.assertIn('exec "$SIM_UPGRADE_SUPERVISOR"', source)
        self.assertIn('SIM_FULLSTACK_MUTATION_SUPERVISED=1', source)
        self.assertIn('FULLSTACK_CONTROL_RC=$?', source)
        self.assertIn("assert_ok 'fullstack source mutation hits", source)
        self.assertNotIn('99-hot-update-fullstack.sh', source)
        for path in (HERE / 'fullstack_mutations.py', HERE / 'fullstack_mutations.json',
                     HERE / 'fullstack_mutations_test.py', HERE.parent / '100-hot-update-fullstack-controls.sh'):
            self.assertLessEqual(len(path.read_text().splitlines()), 700)


if __name__ == '__main__':
    unittest.main(verbosity=2)
