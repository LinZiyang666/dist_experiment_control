#!/usr/bin/env python3
"""Hermetic fixture oracle and driver checks using real custody and IO readers."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from hot_update_transfer_peers import *
from hot_update_transfer_composition import FixtureCompositionTests, native_owner_fixture

class FixtureOracleTests(unittest.TestCase):
    # origin: hot-update S7 plan §10.1 / T42 / M10B.
    def test_partial_destination_restored_is_still_assert_fail(self):
        with live_fixture() as (instance, _, _, target, payload):
            with target.open('r+b') as out:
                out.write(b'X' * (len(payload) // 2))
                out.flush()
                os.fsync(out.fileno())
                instance.keeper.sample()
                out.seek(0)
                out.write(payload)
                out.flush()
                os.fsync(out.fileno())
            self.assertEqual(hashlib.sha256(target.read_bytes()).hexdigest(), hashlib.sha256(payload).hexdigest())
            self.assertEqual(instance.judge()['native_rc'], 1)
            self.assertIn('partial_destination:original', instance.keeper.failures)

    # origin: hot-update S7 plan §10.1 / T42 / M12.
    def test_late_failed_before_judge_is_assert_fail(self):
        with live_fixture() as (instance, _, wire, _, _):
            wire.append('history', 'history.original', canonical('failed'))
            self.assertEqual(instance.judge()['native_rc'], 1)
            self.assertEqual(len(instance.scans['history'].rows), 3)

    # origin: hot-update S7 plan §10.1 / T42 / M12.
    def test_late_failed_after_judge_invalidates_saved_green(self):
        with live_fixture() as (instance, _, wire, _, _):
            self.assertEqual(instance.judge()['native_rc'], 0)
            wire.append('history', 'history.original', canonical('failed'))
            # No sample/judge request is needed: the live raw collector must
            # invalidate the saved receipt while the keeper is still running.
            deadline = time.monotonic() + 3
            while instance.keeper.verdict['native_rc'] == 0 and time.monotonic() < deadline:
                instance.keeper.stop_event.wait(.01)
            self.assertEqual(instance.keeper.verdict['native_rc'], 1)
            saved = json.loads((instance.keeper.evidence / 'verdict.json').read_text())
            self.assertEqual(saved['native_rc'], 1)

    # origin: hot-update S7 plan §10.4 / T42.
    def test_missing_fresh_drain_ack_is_setup_red(self):
        with live_fixture() as (instance, producer, _, _, _):
            first = instance.keeper.drain(timeout=1)
            self.assertEqual(set(first['acks']), {'ctl1'})
            self.assertEqual(len(first['acks']['ctl1']), 32)
            producer.ignore = True
            with self.assertRaisesRegex(observation.SetupError, '^drain_ack_missing$'):
                instance.keeper.drain(timeout=.05)
            self.assertIn('drain_ack_missing', instance.keeper.setup_failures)

    # origin: hot-update S7 plan §10.4 / T42.
    def test_green_requires_two_fresh_drains_and_physical_cursors(self):
        with live_fixture() as (instance, _, _, _, _):
            result = instance.judge()
            self.assertEqual(result['native_rc'], 0)
            self.assertEqual(len({r['acks']['ctl1'] for r in result['drains']}), 2)
            self.assertTrue(all(len(r['acks']['ctl1']) == 32 for r in result['drains']))
            self.assertGreaterEqual(result['drains'][1]['at_ns'] - result['drains'][0]['at_ns'], 2_000_000_000)
            self.assertEqual(result['drains'][1]['streams']['history']['cursor'], 2)

    # origin: hot-update S7 plan §10.4 / T42.
    def test_unjoined_owner_cannot_be_replaced_by_quiet_time(self):
        with live_fixture() as (instance, _, _, _, _):
            with instance.keeper.lock:
                instance.keeper.records.events = [e for e in instance.keeper.records.events if e['kind'] != 'retirement']
            self.assertEqual(instance.judge()['native_rc'], 2)
            self.assertFalse(instance.drains)

    # origin: hot-update S7 plan §10.3 / T42.
    def test_missing_live_cut_window_is_incomplete(self):
        with live_fixture() as (instance, _, _, _, _):
            instance.manifest['cuts'] = [dict(transfer_id='original', leg='leg1', producer='ctl1',
                                               generation=2, role='sender', cut_id='missed')]
            self.assertEqual(instance.judge()['native_rc'], 4)


class CustodyTests(unittest.TestCase):
    # origin: hot-update S7 plan §10.4 / P29.
    def test_sealed_packets_reject_poison_truncation_and_sequence_loss(self):
        identity, incarnation = os.urandom(32), os.urandom(16)
        rows = observation.Records()
        try:
            rows.add(observation.sealed(envelope(identity, incarnation, 1, [event('append', 1)])), 'ctl1', identity)
            for raw in (envelope(identity, incarnation, 3, [event('append', 2)]),
                        envelope(identity, incarnation, 2, [], poison=2),
                        envelope(identity, incarnation, 2, [])[:-1],
                        envelope(identity, incarnation, 2, []) + b'extra'):
                with self.subTest(defect=hashlib.sha256(raw).hexdigest()), self.assertRaises(observation.SetupError):
                    rows.add(observation.sealed(raw), 'ctl1', identity)
        finally:
            rows.close()

    # origin: hot-update S7 plan §10.4 / P29.
    def test_custody_rejects_a_different_immutable_identity(self):
        identity = os.urandom(32)
        records = observation.Records()
        try:
            with self.assertRaisesRegex(observation.SetupError, '^custody_identity_changed$'):
                records.add(observation.sealed(envelope(identity, os.urandom(16), 1, [])),
                            'ctl1', os.urandom(32))
        finally:
            records.close()

    # origin: hot-update S7 plan §10.4 / P29.
    def test_event_gap_and_arbitrary_secret_fields_poison_reader(self):
        identity, incarnation = os.urandom(32), os.urandom(16)
        for row in (event('append', 2), event('append', 1, capsule='secret'), event('append', 1, offset=-1)):
            records = observation.Records()
            try:
                with self.assertRaises(observation.SetupError):
                    records.add(observation.sealed(envelope(identity, incarnation, 1, [row])), 'ctl1', identity)
                self.assertIsNotNone(records.failure)
            finally:
                records.close()

    # origin: hot-update S7 plan §10.4 / P29.
    def test_unsealed_memfd_rejected_and_descriptors_closed(self):
        identity = os.urandom(32)
        records = observation.Records()
        fd = os.memfd_create('unsealed', os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        os.write(fd, envelope(identity, os.urandom(16), 1, []))
        with self.assertRaisesRegex(observation.SetupError, '^custody_unsealed$'):
            records.add(fd, 'ctl1', identity)
        with self.assertRaises(OSError):
            os.fstat(fd)
        records.close()

    # origin: hot-update S7 plan §10.4 / P29.
    def test_five_round_keeper_resources_join_and_close(self):
        before = len(os.listdir('/proc/self/fd'))
        threads = len(threading.enumerate())
        for _ in range(5):
            with live_fixture() as (instance, _, _, _, _):
                instance.keeper.drain(timeout=1)
        self.assertEqual(len(os.listdir('/proc/self/fd')), before)
        self.assertEqual(len(threading.enumerate()), threads)

    # origin: hot-update S7 plan §10.4 / M09.
    def test_ofd_comparison_has_no_same_inode_fallback(self):
        with tempfile.TemporaryFile() as source:
            duplicate = os.dup(source.fileno())
            reopened = os.open('/proc/self/fd/' + str(source.fileno()), os.O_RDWR)
            try:
                try:
                    self.assertTrue(keeper.compare_ofd(os.getpid(), source.fileno(), duplicate))
                except observation.SetupError as error:
                    self.assertEqual(str(error), 'ofd_kernel_unavailable')
                    return  # SETUP2 capability evidence, never an OFD pass.
                self.assertFalse(keeper.compare_ofd(os.getpid(), source.fileno(), reopened))
            finally:
                os.close(duplicate)
                os.close(reopened)


class RawReaderTests(unittest.TestCase):
    # origin: hot-update S7 plan §10.4 / P29 / S7 INTEG-O TDCFG2 migration.
    # Transfer rows are reader controls; native decisions and all drain ACKs
    # come from the original workspace CLI under the shared supervisor.
    def test_live_judge_requires_original_ctl_native_receipt(self):
        with native_owner_fixture() as instance:
            supervised = instance.launcher.children[0][1]
            original = instance.native_results()['ctl1']
            self.assertEqual(original['native_rc'], 0)
            self.assertIsNone(supervised.poll())
            owner = instance.launcher
            instance.launcher = None
            try:
                with self.assertRaisesRegex(observation.SetupError, 'native_ctl_receipt_missing'):
                    instance.judge()
                self.assertIn('native_ctl_receipt_missing', instance.keeper.setup_failures)
            finally:
                instance.launcher = owner
        with native_owner_fixture() as instance:
            identity = instance.keeper.processes['ctl1']
            instance.keeper.processes['ctl1'] = dict(identity, process_start=identity['process_start']+1)
            try:
                with self.assertRaisesRegex(observation.SetupError, 'native_ctl_binding'):
                    instance.judge()
            finally:
                instance.keeper.processes['ctl1'] = identity
            for _ in range(2): instance.drains.append(instance.keeper.drain())
            with instance.keeper.lock:
                frames = instance.keeper.records.frames
                instance.keeper.records.frames = []
                try:
                    with self.assertRaisesRegex(observation.SetupError, 'native_release_not_ready'):
                        instance.release_originals()
                finally:
                    instance.keeper.records.frames = frames
            self.assertIsNone(instance.launcher.children[0][1].poll())
            instance.release_originals()
            self.assertEqual(instance.launcher.children[0][1].poll(), 0)
        with native_owner_fixture(('--definitely-invalid-observer-option',)) as instance:
            original = instance.native_results()['ctl1']
            supervised = instance.launcher.children[0][1]
            self.assertNotEqual(original['native_rc'], 0)
            self.assertEqual(instance.judge()['native_rc'], 3)
            self.assertIsNone(supervised.poll())
            for _ in range(2): instance.drains.append(instance.keeper.drain())
            instance.release_originals()
            self.assertEqual(supervised.poll(), original['native_rc'])
            self.assertEqual(json.loads((instance.keeper.evidence/'native-release.json').read_bytes())['results']['ctl1'], original)

    # origin: hot-update S7 plan §10.3 / P29. Prepared/courier and another leg
    # cannot fill a sender/receiver cell, and a nonempty late cut is still missed.
    def test_cut_requires_same_leg_progress_within_frozen_window(self):
        instance = fixture.Fixture.__new__(fixture.Fixture)
        instance.manifest = dict(transfers=[dict(transfer_id='original', size=100)], cuts=[
            dict(transfer_id='original', leg='leg1', producer='ctl1', generation=2,
                 role='sender', cut_id='cut', max_after_first_ns=30_000_000_000)])
        rows = [event('append', 1, producer='ctl1', leg='leg1', offset=0, length=10, actual_ack_seq=1, boottime_ns=1_000_000_000),
                event('install', 1, producer='ctl1', leg='leg1', generation=2, cut_id='cut', boottime_ns=10_000_000_000),
                event('append', 2, producer='ctl1', leg='leg1', generation=2, offset=10, length=10, actual_ack_seq=2,
                      boottime_ns=11_000_000_000)]
        self.assertEqual(instance.coverage(rows), [])
        rows[1]['boottime_ns'] = 40_000_000_000
        self.assertEqual(instance.coverage(rows), ['cut:original:leg1'])
        rows[1]['boottime_ns'] = 10_000_000_000
        rows[2]['leg'] = 'leg2'
        self.assertEqual(instance.coverage(rows), ['cut:original:leg1'])

    # origin: hot-update S7 plan §10.4 / P29.
    def test_foreign_metadata_and_duplicate_chunk_are_rejected(self):
        with live_fixture() as (instance, _, _, _, _):
            instance.scan()
            transfer = instance.manifest['transfers'][0]
            events = instance.keeper.records.events
            original = instance.scans['OBJ_bucket'].rows
            self.assertTrue(oracle.judge_object(original, transfer, events))
            foreign = copy.deepcopy(original)
            meta = json.loads(base64.b64decode(foreign[-1]['data_b64']))
            meta['nuid'] = 'foreign'
            foreign[-1]['data_b64'] = base64.b64encode(json.dumps(meta).encode()).decode()
            self.assertFalse(oracle.judge_object(foreign, transfer, events))
            duplicate = copy.deepcopy(original)
            duplicate.insert(1, copy.deepcopy(duplicate[0]))
            self.assertFalse(oracle.judge_object(duplicate, transfer, events))
            changed = copy.deepcopy(events)
            changed[0]['actual_ack_seq'] += 1
            self.assertFalse(oracle.judge_object(original, transfer, changed))

    # origin: hot-update S7 plan §10.4 / P29.
    def test_pipeline_reads_original_physical_sequences(self):
        with live_fixture() as (instance, _, wire, _, _):
            for index in range(70):
                wire.append('history', 'history.other', str(index).encode())
            instance.scan()
            rows = instance.scans['history'].rows
            self.assertEqual([r['sequence'] for r in rows], list(range(1, 73)))
            self.assertTrue(oracle.judge_history(rows, instance.manifest['transfers'][0]))

    # origin: hot-update S7 plan §10.4 / P29.
    def test_retained_physical_record_is_independently_rechecked(self):
        with live_fixture() as (instance, _, wire, _, _):
            instance.scan()
            with wire.lock:
                wire.rows['history'][1] = ('history.original', canonical('failed'))
            with oracle.NATSReader(wire.url) as reader, self.assertRaisesRegex(
                    observation.SetupError, '^physical_row_changed$'):
                instance.scans['history'].verify_retained(reader, {'history.original'})

    # origin: hot-update S7 plan §10.4 / P29.
    def test_stream_recreation_and_cursor_reversal_are_setup_red(self):
        with live_fixture() as (instance, _, wire, _, _):
            instance.scan()
            scanner = instance.scans['history']
            scanner.created = 'old-created'
            with oracle.NATSReader(wire.url) as reader, self.assertRaisesRegex(
                    observation.SetupError, '^stream_recreated$'):
                scanner.scan(reader)
            scanner.created = 'fixed-created'
            scanner.cursor = 1000
            with oracle.NATSReader(wire.url) as reader, self.assertRaisesRegex(
                    observation.SetupError, '^stream_cursor_reversed$'):
                scanner.scan(reader)

    # origin: hot-update S7 plan §10.3 / P29.
    def test_read_manifest_rejects_shared_transfer_ownership(self):
        with live_fixture() as (instance, _, _, _, _):
            manifest = copy.deepcopy(instance.manifest)
            manifest['transfers'].append(copy.deepcopy(manifest['transfers'][0]))
            path = instance.keeper.evidence / 'bad-manifest.json'
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(observation.SetupError, '^transfer_isolation$'):
                fixture.read_manifest(path)

    # origin: hot-update S7 plan §10.4 / P29.
    def test_target_reader_never_follows_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            source.write_bytes(b'complete')
            target = root / 'target'
            target.symlink_to(source)
            reader = oracle.TargetOracle(target, None, [8, hashlib.sha256(b'complete').hexdigest()])
            with self.assertRaises(OSError):
                reader.sample()


class FixtureDriverTests(unittest.TestCase):
    # origin: hot-update S6 plan §10.1 / T58; DRILL-ASSETS / startup receipt is live and bound.
    def test_fixture_startup_wait_rejects_dead_or_foreign_receipt(self):
        with tempfile.TemporaryDirectory(prefix='transfer-startup-') as directory:
            root = Path(directory)
            manifest, log, receipt = root / 'manifest.json', root / 'start.out', root / 'start.rc'
            manifest.write_text('{}\n')
            receipt.write_text('')  # The Go launcher opens its receipt before spawning.
            ready = dict(fixture_pid=os.getpid(), manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
                         custody_refs=dict(control=str(root / 'control.sock')))
            log.write_text(json.dumps(ready) + '\n')
            fixture_manifest.wait_start(manifest, log, receipt, os.getpid(), .1)
            for defect in ('digest', 'control', 'pid', 'exited', 'missing'):
                with self.subTest(defect=defect):
                    value = copy.deepcopy(ready)
                    receipt.write_text('126\n' if defect == 'exited' else '')
                    if defect == 'digest': value['manifest_sha256'] = 'a' * 64
                    elif defect == 'control': value['custody_refs']['control'] = '/foreign/control.sock'
                    elif defect == 'pid': value['fixture_pid'] = 0
                    log.write_text('' if defect == 'missing' else json.dumps(value) + '\n')
                    with self.assertRaises(ValueError):
                        fixture_manifest.wait_start(manifest, log, receipt, os.getpid(), .03)

    # origin: hot-update S6 plan §10.1 / T58; DRILL-ASSETS / both directions of control matching.
    def test_control_runner_requires_zero_setup_and_bound_kill_receipt(self):
        spec = importlib.util.spec_from_file_location('transfer_driver_controls',
            Path(__file__).with_name('hot-update-transfer-drill-test.py'))
        controls = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(controls)
        box = controls.Sandbox('M01')
        try:
            logdir = box.root / 'logs'
            evidence = logdir / 'evidence' / 'original-control'
            evidence.mkdir(parents=True)
            patch = controls.production_patch(box.repo, 'M01')
            controls.tm.apply_overlay(box.repo, evidence / 'mutation', 'M01', patch)
            (evidence / 'mutation/tether').write_bytes(b'hermetic archived mutant; never executed')
            artifact = controls.sha((evidence / 'mutation/tether').read_bytes())
            (evidence / 'mutation/build.json').write_text(json.dumps(dict(artifact_sha=artifact)))
            (evidence / 'judge.out').write_text(controls.native_line('ASSERT-FAIL', 1))
            (box.fake / 'archive.json').write_text('{}')
            subprocess.run(['python3', '-I', '-B', str(box.fake / 'archive.py'),
                            str(box.fake / 'archive.json'), str(evidence)], check=True, timeout=10)
            receipt = controls.tm.check_kill(evidence, 'M01', 1)
            # A fully valid receipt from a different directory is still foreign
            # to this invocation. A missing-file example alone cannot test the
            # directory guard because the later reader would reject it too.
            foreign = box.root / 'foreign-control'
            foreign.mkdir()
            controls.tm.apply_overlay(box.repo, foreign / 'mutation', 'M01', patch)
            (foreign / 'mutation/tether').write_bytes((evidence / 'mutation/tether').read_bytes())
            (foreign / 'mutation/build.json').write_text(json.dumps(dict(artifact_sha=artifact)))
            (foreign / 'judge.out').write_text(controls.native_line('ASSERT-FAIL', 1))
            subprocess.run(['python3', '-I', '-B', str(box.fake / 'archive.py'),
                            str(box.fake / 'archive.json'), str(foreign)], check=True, timeout=10)
            (foreign / 'control-receipt.json').write_text(json.dumps(controls.tm.check_kill(foreign, 'M01', 1)))
            unit = '64-hot-update-transfer-controls.M01'
            table = logdir / 'expected.tsv'
            table.write_text(unit + '\tASSERT-FAIL\t0\t-\t-\thermetic control\n' +
                             'unrelated\tASSERT-FAIL\t0\t-\t-\tunrelated expectation\n')
            source = (LIB.parents[1] / 'run-drills.sh').read_text()
            functions = ''
            for name in ('_exp_field', '_sig_regex', '_fail_context', 'transfer_control_receipt_matches', 'classify_match'):
                match = re.search(r'^' + re.escape(name) + r'\(\) \{.*?^}\n', source, re.M | re.S)
                self.assertIsNotNone(match, name)
                functions += match.group()
            frame = 'HERE="$1"; LOGDIR="$2"; EXPECTED_TSV="$3"; EXPECTED_LOG=/missing\n'
            for variant, expected in (('valid', 'MATCH'), ('setup', 'DEVIATION'),
                    ('missing', 'DEVIATION'), ('changed', 'DEVIATION'), ('unexercised', 'DEVIATION'),
                    ('foreign-directory', 'DEVIATION'), ('valid-again', 'MATCH'), ('unrelated', 'MATCH')):
                with self.subTest(variant=variant):
                    path = evidence / 'control-receipt.json'
                    path.write_text(json.dumps(dict(receipt, killed=False) if variant == 'changed' else receipt))
                    if variant == 'missing': path.unlink()
                    witness_path = evidence / 'mutation-witness.json'
                    witness = json.loads(witness_path.read_text())
                    witness['exercised'] = variant != 'unexercised'
                    witness_path.write_text(json.dumps(witness))
                    setup = 1 if variant in ('setup', 'unrelated') else 0
                    current = 'unrelated' if variant == 'unrelated' else unit
                    directory = foreign if variant == 'foreign-directory' else evidence
                    (logdir / (current + '.log')).write_text('TRANSFER-EVIDENCE directory=' + str(directory) + '\n' +
                        controls.native_line('ASSERT-FAIL', 1).replace('setup_red=0', 'setup_red=' + str(setup)))
                    result = subprocess.run(['bash', '-c', frame + functions + '\nclassify_match "$4" ASSERT-FAIL 0 ""\n',
                        'control-match', str(LIB.parents[1]), str(logdir), str(table), current],
                        capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stdout, expected, result.stderr)
        finally:
            box.close()

    # origin: hot-update S6 plan §10.1 / T58; DRILL-ASSETS / real schema and foreground start seam.
    def test_driver_starts_and_joins_real_fixture_with_frozen_schema(self):
        spec = importlib.util.spec_from_file_location('transfer_driver_controls',
            Path(__file__).with_name('hot-update-transfer-drill-test.py'))
        controls = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(controls)
        supervisor = os.environ.get('SIM_UPGRADE_SUPERVISOR')
        self.assertTrue(supervisor and Path(supervisor).is_file(), 'shared Go launcher must be provisioned by selftest wrapper')
        with tempfile.TemporaryDirectory(prefix='transfer-start-') as directory:
            root = Path(directory)
            wire = ProtocolServer(b'hermetic wire data')
            try:
                for arm in ('A', 'M01'):
                    with self.subTest(arm=arm):
                        box = controls.Sandbox(arm)
                        try:
                            # Real startup reads only explicit private sandbox paths.
                            deployment = copy.deepcopy(box.deployment)
                            for i, cohort in enumerate(deployment['cohorts']):
                                base = root / arm / str(i)
                                base.mkdir(parents=True)
                                for field in ('home', 'binary_path', 'source', 'destination'):
                                    cohort[field] = str(base / field)
                            deployment['fixture'] = controls.fixture_deployment(deployment, arm, 'single')
                            deployment['fixture']['nats']['url'] = wire.url
                            with wire.lock:
                                wire.rows['history'] = []
                                for transfer in deployment['fixture']['transfers']:
                                    wire.rows[transfer['object_stream']] = []
                            plan = controls.tm.make_manifest(deployment, arm)
                            if arm == 'M01':
                                # make_manifest also retains a preexisting flat
                                # fixture deployment with its schema overwritten.
                                flat = plan.pop('fixture')
                                plan.update({key: value for key, value in flat.items() if key != 'schema'})
                            evidence = root / arm / 'evidence'
                            evidence.mkdir()
                            driver_path, fixture_path = evidence / 'manifest.json', evidence / 'fixture-manifest.json'
                            driver_path.write_text(json.dumps(plan, sort_keys=True) + '\n')
                            fixture_manifest.write_manifest(driver_path, fixture_path)
                            frozen = fixture.read_manifest(fixture_path)
                            self.assertEqual(frozen['schema'], 1)
                            self.assertEqual(frozen['mode'], 'live')
                            self.assertNotIn('steps', frozen)
                            expected_shas = sorted({a['sha256'] for a in plan['artifacts'].values()})
                            self.assertTrue(all(p['artifact_shas'] == expected_shas for p in frozen['processes']))
                            self.assertEqual([t['transfer_id'] for t in frozen['transfers']],
                                             [t['transfer_id'] for t in deployment['fixture']['transfers']])
                            shell = '''HERE="$1"; . "$HERE/drills/lib/hot_update_transfer.sh"
TRANSFER_EV="$2"; TRANSFER_MANIFEST="$2/manifest.json"; TRANSFER_FIXTURE_MANIFEST="$2/fixture-manifest.json"
TRANSFER_PROCESS_SUPERVISOR="$3"
trap 'if [ -n "$TRANSFER_FIXTURE_JOB" ]; then kill -TERM "$TRANSFER_FIXTURE_JOB" 2>/dev/null || true; wait "$TRANSFER_FIXTURE_JOB" || true; fi' EXIT
transfer_fixture start || exit 91
kill -0 "$TRANSFER_FIXTURE_JOB" || exit 92
printf '{"native_rc":2,"setup":["hermetic_start_only"]}\\n' > "$TRANSFER_EV/failure.json"
transfer_fixture stop; rc=$?
[ "$rc" = 2 ] || exit 93
[ -z "$TRANSFER_FIXTURE_JOB" ] || exit 94
[ ! -e "$TRANSFER_EV/control.sock" ] || exit 95
'''
                            result = subprocess.run(['sh', '-c', shell, 'fixture-start-control',
                                str(LIB.parents[1]), str(evidence), supervisor],
                                env=dict(os.environ, HOME=str(root)), capture_output=True, text=True, timeout=30)
                            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                            ready = json.loads((evidence / 'fixture-start.out').read_text())
                            self.assertEqual(ready['manifest_sha256'], hashlib.sha256(fixture_path.read_bytes()).hexdigest())
                            self.assertEqual((evidence / 'fixture-start.rc').read_text(), '2\n')
                            with self.assertRaises(ProcessLookupError):
                                os.kill(ready['fixture_pid'], 0)
                        finally:
                            box.close()
            finally:
                wire.close()
            self.assertFalse(wire.errors, wire.errors)

    # origin: hot-update S6 plan §10.1 / T58; DRILL-ASSETS / preserve deployment identity and rate.
    def test_fixture_projection_rejects_missing_or_drifting_deployment(self):
        spec = importlib.util.spec_from_file_location('transfer_driver_controls',
            Path(__file__).with_name('hot-update-transfer-drill-test.py'))
        controls = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(controls)
        box = controls.Sandbox('A')
        try:
            for defect in ('missing', 'schema', 'mode', 'rate', 'session', 'canonical', 'installation'):
                with self.subTest(defect=defect):
                    plan = copy.deepcopy(box.plan)
                    if defect == 'missing': del plan['fixture']
                    elif defect == 'schema': plan['fixture']['schema'] = 'transfer-driver-v1'
                    elif defect == 'mode': plan['fixture']['mode'] = 'hermetic'
                    elif defect == 'rate': plan['fixture']['transfers'][0]['rate_limit'] += 1
                    elif defect == 'session': plan['fixture']['transfers'][0]['session_id'] = 'foreign'
                    elif defect == 'canonical': plan['fixture']['transfers'][0]['start_b64'] = base64.b64encode(canonical('start')).decode()
                    else: plan['fixture']['processes'][0]['home'] = '/foreign/home'
                    path = box.root / 'bad-driver.json'
                    output = box.root / 'bad-fixture.json'
                    path.write_text(json.dumps(plan))
                    with self.assertRaises(ValueError):
                        fixture_manifest.write_manifest(path, output)
                    self.assertFalse(output.exists())
                    self.assertFalse(output.with_suffix('.pending').exists())
        finally:
            box.close()


if __name__ == '__main__':
    unittest.main()
