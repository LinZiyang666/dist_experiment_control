"""Real fixture protocol composition; product peers are explicit hermetic IO peers.

No supervisor or archive writer is replaced here. These are reader/driver
receipts, not evidence that any deployed transfer or production mutant ran.
"""
from hot_update_transfer_peers import *
from transfer_mutations import check_sample, check_kill, make_manifest, apply_overlay, load
from hot_update_transfer_doubles import Sandbox, production_patch


@contextlib.contextmanager
def native_owner_fixture(arguments=('version',)):
    """Real TDCFG2 writer/native result; transfer rows remain reader controls.

    The workspace CLI is supervised and keeps its own custody until TDR2.
    ProtocolServer and the transfer rows exercise the reader, not a deployed
    transfer. No native receipt or drain ACK is supplied by the Python peer.
    """
    with tempfile.TemporaryDirectory(prefix='transfer-native-owner-') as directory:
        root, payload = Path(directory), b'original physical bytes'
        binary = root / 'observer-cli'
        supervisor = os.environ['SIM_UPGRADE_SUPERVISOR']
        repo = Path(__file__).resolve().parents[3]
        build = subprocess.run([supervisor, '--', 'go', 'build', '-o', str(binary), './cmd/tether'],
                               cwd=repo, capture_output=True, text=True, timeout=180)
        assert build.returncode == 0, build.stdout + build.stderr
        wire = ProtocolServer(payload)
        target = root / 'target'
        target.write_bytes(payload)
        transfer = dict(transfer_id='original', entry_nonce='nonce', object_version='NUID', object_name='object',
            bucket='bucket', session_id='session', cohort='hermetic', target=str(target), old=None,
            size=len(payload), sha256=hashlib.sha256(payload).hexdigest(), object_stream='OBJ_bucket',
            history_stream='history', history_subject='history.original', required_resources=[], ctl_producer='ctl1',
            start_b64=base64.b64encode(canonical('start')).decode(),
            terminal_b64=base64.b64encode(canonical('complete')).decode())
        spec = dict(label='ctl1', role='ctl', artifact_shas=[hashlib.sha256(binary.read_bytes()).hexdigest()],
                    launch=dict(home=str(root/'private-home'), argv=[str(binary), *arguments]))
        manifest = dict(schema=1, mode='hermetic', quiet_seconds=2, cuts=[], processes=[spec],
                        transfers=[transfer], nats=dict(url=wire.url), manifest_sha256='a'*64)
        instance = fixture.Fixture(manifest, root/'evidence')
        instance.reader_control_peer = wire
        instance.launcher = fixture_manifest.LaunchOwner(instance)
        with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as service:
            service.bind(str(instance.keeper.evidence/'control.sock'))
            service.listen(1)
            service.settimeout(5)
            failures = []
            def lease():
                try:
                    channel, _ = service.accept()
                    with channel:
                        request = json.loads(channel.recv(65536))
                        assert request == dict(command='lease', label='ctl1', manifest_sha256='a'*64)
                        producer, config = instance.keeper.lease('ctl1')
                        try:
                            assert os.pread(config, 8, 0) == b'TDCFG2\0\0'
                            channel.sendmsg([b'{"native_rc":0}'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                array.array('i', [producer.fileno(), config]))])
                        finally:
                            producer.close()
                            os.close(config)
                except BaseException as error:
                    failures.append(error)
            thread = threading.Thread(target=lease, name='native-owner-lease')
            thread.start()
            try:
                instance.keeper.start()
                instance.launcher.launch(spec)
                thread.join(5)
                assert not thread.is_alive() and not failures, failures
                deadline = time.monotonic()+5
                while True:
                    with instance.keeper.lock:
                        frames = instance.keeper.records.frames
                        if frames:
                            raw = os.pread(frames[0], os.fstat(frames[0]).st_size, 0)
                            offset = 76
                            offset += 1+raw[offset]
                            offset += 1+raw[offset]
                            native = json.loads(raw[offset+232:])
                            identity = keeper.process_identity(native['pid'])
                            assert identity['running_sha'] == spec['artifact_shas'][0]
                            instance.keeper.processes['ctl1'] = identity
                            break
                    assert time.monotonic() < deadline, 'original native-exit custody missing'
                    time.sleep(.01)
                # These controlled rows supply only the pre-existing transfer
                # reader predicates. Native result/ACK/release are all Go IO.
                instance.keeper.records.events = [
                    event('append', 1, producer='ctl1', index=0, offset=0, length=len(payload),
                          sha256=hashlib.sha256(payload).hexdigest(), actual_ack_seq=1),
                    event('primitive_exit', 2, producer='ctl1', primitive='rename', outcome='landed'),
                    event('directory_sync', 3, producer='ctl1', parent_synced=True),
                    event('retirement', 4, producer='ctl1', admission_closed=True,
                          retirement_digest='d'*64, stop_nonce='joined')]
                instance.manifest['mode'] = 'live'
                yield instance
            finally:
                # A failed native join still owns the remaining protocol peer.
                # origin: S7 INTEG-O TDCFG2 migration / S7 plan lead L16
                try:
                    instance.close()
                finally:
                    try:
                        thread.join(5)
                        assert not thread.is_alive(), 'native owner lease did not join'
                    finally:
                        wire.close()
                        assert not wire.errors, wire.errors


@contextlib.contextmanager
def running_fixture(matrix=False, late=False, silence=False, launched=False):
    """Launch the real five-verb CLI with the shared supervisor and join it."""
    supervisor = os.environ['SIM_UPGRADE_SUPERVISOR']
    with tempfile.TemporaryDirectory(prefix='transfer-composition-') as directory:
        root = Path(directory)
        box = Sandbox('M01') if matrix else None
        payload = b'original physical bytes'
        wire = ProtocolServer(payload)
        identity = keeper.process_identity(os.getpid())
        evidence = root / 'evidence'
        evidence.mkdir()
        if box:
            plan = make_manifest(box.deployment, 'M01')
            manifest = copy.deepcopy(plan['fixture'])
            manifest.update(mode='hermetic', driver=plan,
                            driver_sha=hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest())
            manifest.pop('control')
            # Four declared large tasks with deliberately truncated physical
            # effects exercise a real ASSERT-FAIL archive. No large-file GREEN
            # or product mutation receipt is claimed by this hermetic peer.
            manifest['processes'] = [dict(label='agent'+str(i), role='ctl', artifact_shas=[identity['running_sha']])
                                     for i in range(4)]
            for i, transfer in enumerate(manifest['transfers']):
                transfer.update(target=str(root / ('target'+str(i))), required_resources={})
        else:
            plan = None
            manifest = dict(schema=1, mode='hermetic', quiet_seconds=2, cuts=[],
                processes=[dict(label='ctl1', role='ctl', artifact_shas=[identity['running_sha']])],
                transfers=[dict(transfer_id='original', entry_nonce='nonce', object_version='NUID', object_name='object',
                    bucket='bucket', session_id='session', cohort='hermetic', target=str(root/'target'),
                    size=len(payload), sha256=hashlib.sha256(payload).hexdigest(), object_stream='OBJ_bucket',
                    history_stream='history', history_subject='history.original', required_resources={})])
        manifest['nats'] = dict(url=wire.url)
        with wire.lock:
            wire.rows = {'history': []}
            for transfer in manifest['transfers']:
                Path(transfer['target']).write_bytes(payload)
                for kind in ('start', 'complete'):
                    raw = json.dumps(dict(transfer_id=transfer['transfer_id'], kind=kind)).encode()
                    wire.rows['history'].append((transfer['history_subject'], raw))
                transfer.pop('start_b64', None)
                transfer.pop('terminal_b64', None)
                metadata = dict(nuid=transfer['object_version'], size=len(payload), chunks=1,
                    digest='SHA-256='+base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).decode())
                name = base64.urlsafe_b64encode(transfer['object_name'].encode()).decode().rstrip('=')
                wire.rows[transfer['object_stream']] = [('$O.'+transfer['bucket']+'.C.'+transfer['object_version'],payload),
                    ('$O.'+transfer['bucket']+'.M.'+name,json.dumps(metadata).encode())]
        if launched:
            code = ('import sys; sys.path.insert(0,'+repr(str(Path(__file__).parent))+'); '
                    'from hot_update_transfer_peers import launched_custody_peer; launched_custody_peer()')
            manifest['launch_owners'] = True
            manifest['processes'][0]['launch'] = dict(home=str(root), argv=[sys.executable,'-I','-B','-c',code])
        manifest_path = evidence/'fixture-manifest.json'
        manifest_path.write_text(json.dumps(manifest))
        manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        if matrix:
            patch = production_patch(box.repo, 'M01')
            apply_overlay(box.repo, evidence/'mutation', 'M01', patch)
            # Bind the actual peer executable to the archive. This is a wire
            # reader control, not proof that Python implements a Go mutation.
            (evidence/'mutation/tether').write_bytes(Path('/proc/self/exe').read_bytes())
            (evidence/'mutation/build.json').write_text(json.dumps(dict(artifact_sha=identity['running_sha'])))
        command = [sys.executable,'-I','-B',str(LIB/'transfer_fixture.py')]
        with (evidence/'start.out').open('wb') as out:
            child = subprocess.Popen([supervisor,'--',*command,'start','--manifest',str(manifest_path),
                                      '--evidence',str(evidence)], stdout=out, stderr=out)
        producers=[]
        try:
            deadline=time.monotonic()+5
            while not (evidence/'control.sock').exists() and time.monotonic()<deadline:
                assert child.poll() is None, (evidence/'start.out').read_text()
                time.sleep(.01)
            assert (evidence/'control.sock').exists()
            for spec, transfer in ([] if launched else zip(manifest['processes'],manifest['transfers'])):
                with socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET) as channel:
                    channel.settimeout(5)
                    channel.connect(str(evidence/'control.sock'))
                    channel.send(json.dumps(dict(command='lease',label=spec['label'],manifest_sha256=manifest_sha)).encode())
                    raw,fds=observation.receive(channel)
                    assert json.loads(raw)['native_rc']==0 and len(fds)==2
                config=os.pread(fds[1],32,0)
                os.close(fds[1])
                peer=Producer(socket.socket(fileno=fds[0]),hashlib.sha256(config).digest())
                producers.append(peer)
                def row(kind, seq, transfer=transfer, **extra):
                    return event(kind,seq, transfer_id=transfer['transfer_id'],entry_nonce=transfer['entry_nonce'],
                        object_version=transfer['object_version'],leg='leg1' if matrix else 'leg2',**extra)
                rows=[row('append',1,index=0,offset=0,length=len(payload),actual_ack_seq=1,
                          sha256=hashlib.sha256(payload).hexdigest())]
                rows += [row('control',2,phase='prepared'),row('acknowledge',3),
                         row('primitive_exit',4,primitive='rename',outcome='landed'),
                         row('directory_sync',5,parent_synced=True),
                         row('retirement',6,admission_closed=True,retirement_digest='d'*64,stop_nonce='joined')]
                peer.emit(rows)
                def at_drain(number, peer=peer, row=row):
                    # transfer_finish calls one drain, judge two, then stop one.
                    if number==4 and late:
                        peer.emit([row('finish',7,canonical_digest='f'*64)])
                    if number==4 and silence:
                        peer.ignore=True
                peer.on_drain=at_drain
                peer.thread.start()
            def invoke(verb):
                return subprocess.run([supervisor,'--',*command,verb,'--manifest',str(manifest_path),
                    '--evidence',str(evidence)],capture_output=True,text=True,timeout=30)
            yield root,evidence,plan,invoke,child
        finally:
            if child.poll() is None:
                child.terminate()
            try: child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill(); child.wait()
            for peer in producers: peer.close()
            wire.close()
            if box: box.close()
            assert not wire.errors,wire.errors


class ControlSlotClockOwner(observation.ControlOwner):
    """Share a stepped slot clock with the real command peer over a file."""
    def __init__(self, instance):
        super().__init__(instance)
        self.clock_path = instance.keeper.evidence / 'control-slot-clock.ns'
        self.baseline = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
        self.clock_path.write_text(str(self.baseline))
        self.completed = threading.Condition()

    def now(self):
        return int(self.clock_path.read_text())

    def advance(self, elapsed_ns):
        # Atomic replacement prevents a child from observing an empty clock.
        # Time advances only after the previous command has exited and its
        # real attempt census and effect have been read by the owner.
        pending = self.clock_path.with_suffix('.pending')
        pending.write_text(str(self.baseline + elapsed_ns))
        pending.replace(self.clock_path)

    def reap(self, entry, expired=False):
        with self.completed:
            super().reap(entry, expired)
            self.completed.notify_all()

    def await_finished(self, count):
        with self.completed:
            return self.completed.wait_for(lambda: len(self.finished) >= count, timeout=5)


class FixtureCompositionTests(unittest.TestCase):
    # origin: S7 INTEG-O TDCFG2 migration / S7 plan lead L16.
    def test_native_close_failure_still_joins_protocol_peer(self):
        before = len(os.listdir('/proc/self/fd'))
        threads = len(threading.enumerate())
        for _ in range(5):
            peer = None
            try:
                with self.assertRaisesRegex(observation.SetupError, 'forced_native_join_failure'):
                    with native_owner_fixture() as instance:
                        peer = instance.reader_control_peer
                        original_close = instance.close
                        def failed_close():
                            original_close()
                            raise observation.SetupError('forced_native_join_failure')
                        instance.close = failed_close
                        for _ in range(2): instance.drains.append(instance.keeper.drain())
                        instance.release_originals()
                self.assertFalse(peer.thread.is_alive())
                self.assertEqual(len(threading.enumerate()), threads)
            finally:
                if peer is not None: peer.close()
        self.assertEqual(len(os.listdir('/proc/self/fd')), before)

    # origin: S7 internal review rv-drill F1.
    def test_real_fixture_writes_scheduler_native_and_mutation_evidence(self):
        with running_fixture(matrix=True) as (_,evidence,plan,invoke,_):
            sample=invoke('sample')
            # The independently observed truncated targets already assert red,
            # but must still deliver the scheduler's exact sample protocol.
            self.assertEqual(sample.returncode,1,sample.stdout+sample.stderr)
            value=load(evidence/'sample.json')
            self.assertEqual(check_sample(plan,value,plan['steps'][0]['id']),0)
            judged=invoke('judge')
            self.assertEqual(judged.returncode,1,judged.stdout+judged.stderr)
            (evidence/'judge.out').write_text(judged.stdout)
            receipt=check_kill(evidence,'M01',1)
            self.assertTrue(receipt['killed'])
            for name in ('mutation-runtime.json','mutation-witness.json','judge.out'):
                path=evidence/name
                original=path.read_bytes()
                try:
                    if name=='judge.out': path.write_text('{"native_rc":1}')
                    else: path.unlink()
                    with self.assertRaises((ValueError,OSError,KeyError)):
                        check_kill(evidence,'M01',1)
                finally: path.write_bytes(original)
            runtime=evidence/'mutation-runtime.json'
            original=runtime.read_bytes()
            try:
                value=json.loads(original)
                for process in value['processes']:
                    process.update(artifact_sha=None,running_sha=None)
                runtime.write_text(json.dumps(value))
                with self.assertRaises(ValueError):
                    check_kill(evidence,'M01',1)
            finally:
                runtime.write_bytes(original)
            self.assertEqual(invoke('stop').returncode,1)

    # origin: S7 internal review rv-drill F8. No drill entrypoint is executed;
    # actual driver functions call the real fixture over its real control socket.
    def test_driver_waits_for_late_poison_and_missing_final_ack(self):
        for defect,expected in (('late',1),('silence',2)):
            with self.subTest(defect=defect), running_fixture(late=defect=='late',silence=defect=='silence') as (
                    _,evidence,_,_,child):
                shell='''HERE="$1"; . "$HERE/lib/log.sh"; . "$HERE/lib/assert.sh"
. "$HERE/drills/lib/hot_update_transfer.sh"
TRANSFER_EV="$2"; TRANSFER_FIXTURE_MANIFEST="$2/fixture-manifest.json"
TRANSFER_PROCESS_SUPERVISOR="$3"; TRANSFER_STARTED=1; ARM=A
INSTANCE=drill-transfer-stop; drill_begin transfer-stop
transfer_finish
drill_end
'''
                result=subprocess.run([os.environ['SIM_UPGRADE_SUPERVISOR'],'--','sh','-c',shell,
                    'transfer-stop',str(LIB.parents[1]),str(evidence),os.environ['SIM_UPGRADE_SUPERVISOR']],
                    capture_output=True,text=True,timeout=30)
                self.assertEqual(result.returncode,expected,result.stdout+result.stderr)
                self.assertEqual((evidence/'initial-judge.rc').read_text(),'0\n')
                self.assertEqual(len(re.findall(r'^DRILL-VERDICT ',result.stdout+result.stderr,re.M)),1,result.stdout+result.stderr)
                self.assertNotIn('verdict=GREEN',result.stdout+result.stderr)
                self.assertIn('DRILL-VERDICT verdict='+('ASSERT-FAIL' if expected==1 else 'SETUP-RED'),
                              result.stdout+result.stderr)
                self.assertEqual(child.wait(timeout=5),expected)

    # origin: S7 internal review rv-drill F1. Launch custody after supervision;
    # the observed PID belongs to the exec'ed child, not this test interpreter.
    def test_launch_owner_leases_custody_to_original_supervised_process(self):
        with running_fixture(launched=True) as (_,evidence,_,invoke,child):
            deadline=time.monotonic()+5
            while time.monotonic()<deadline:
                self.assertEqual(invoke('sample').returncode,0)
                snapshot=load(evidence/'snapshot.json')
                if snapshot['events']: break
                time.sleep(.01)
            self.assertTrue(snapshot['events'])
            self.assertNotEqual(snapshot['events'][0]['pid'],os.getpid())
            self.assertEqual(invoke('judge').returncode,0,(evidence/'verdict.json').read_text()+(evidence/'start.out').read_text()+
                             ''.join(p.read_text() for p in evidence.glob('owner-*.out')))
            self.assertEqual(invoke('stop').returncode,0)
            self.assertEqual(child.wait(timeout=5),0)

    # origin: S7 internal review rv-drill F6. Real timer, supervisor, attempt
    # sink and independent effect reader; no precomputed slot list is supplied.
    # origin: S7 internal review INTEG-O F9
    def test_control_owner_generates_slots_and_reads_real_command_effects(self):
        with live_fixture() as (instance,_,_,_,_):
            # A small independent command avoids importing the whole test suite
            # into each 100ms slot. The real supervisor/bootstrap is unchanged.
            code='''import os,sys,json,time
from pathlib import Path
logical,target,clock=sys.argv[1:]
start=int(Path(clock).read_text())
Path(target).write_text(logical+'\\n')
a=dict(logical_id=logical,ordinal=1,phase='result',started_mono_ns=start,
finished_mono_ns=int(Path(clock).read_text()),sent=True,backoff_ns=0)
a['class']='ok'
tail=dict(kind='observer_close',logical_id=logical,attempt_count=1,overflow=False)
os.write(int(os.environ['TETHER_ATTEMPT_RECEIPT_FD']),(json.dumps(a)+'\\n'+json.dumps(tail)+'\\n').encode())
'''
            instance.manifest['control']=dict(launch=dict(home=str(instance.keeper.evidence),
                argv=[sys.executable,'-I','-B','-c',code,'{logical_id}','{effect}',
                      str(instance.keeper.evidence / 'control-slot-clock.ns')]))
            owner=ControlSlotClockOwner(instance)
            instance.controls=owner
            owner.start()
            try:
                # 100ms is a strict live slot budget. This hermetic composition
                # test advances the clock shared by the scheduler and command
                # after real IO completion, so host startup latency cannot
                # impersonate a late product slot. No receipt is rewritten.
                for slot in range(3):
                    owner.advance(slot * 100_000_000)
                    self.assertTrue(owner.await_finished(slot + 1), instance.keeper.setup_failures)
                owner.advance(240_000_000)
            finally:
                owner.close()
            self.assertFalse(instance.keeper.setup_failures)
            self.assertGreaterEqual(len(owner.control['logical_ids']),3)
            self.assertEqual(owner.control['logical_ids'], ['control-0', 'control-1', 'control-2'])
            oracle.judge_control(owner.control,owner.attempts,owner.effects)
            # The same strict oracle rejects the exact 100ms boundary even
            # when every original command exited successfully with one effect.
            late = copy.deepcopy(owner.attempts)
            late[0]['attempt']['started_mono_ns'] = owner.control['baseline_ns'] + 100_000_000
            late[0]['attempt']['finished_mono_ns'] = late[0]['attempt']['started_mono_ns']
            with self.assertRaisesRegex(observation.SetupError, '^control_slot_late$'):
                oracle.judge_control(owner.control,late,owner.effects)
            missing=dict(owner.effects)
            del missing[owner.control['logical_ids'][0]]
            with self.assertRaises(observation.SetupError):
                oracle.judge_control(owner.control,owner.attempts,missing)

    # origin: S7 internal review rv-drill F8. A failed owner join cannot retain
    # the earlier GREEN even when final custody and raw scans are complete.
    def test_stop_propagates_owner_join_failure(self):
        with live_fixture() as (instance,_,_,_,_):
            self.assertEqual(instance.judge()['native_rc'],0)
            original=instance.close
            def failed_join():
                original()
                raise observation.SetupError('collector_join')
            instance.close=failed_join
            try:
                self.assertEqual(instance.stop()['native_rc'],2)
                self.assertIn('fixture_join_failed',instance.keeper.setup_failures)
            finally:
                instance.close=original

    # origin: S7 internal review rv-drill F8. A failure in the first closer must
    # still join the raw-reader/custody threads and every later owner.
    def test_close_joins_remaining_owners_after_control_failure(self):
        with live_fixture() as (instance,_,_,_,_):
            class BrokenControl:
                def close(self):
                    raise observation.SetupError('control_slot_join')
            instance.controls=BrokenControl()
            try:
                with self.assertRaises(observation.SetupError): instance.close()
                self.assertFalse(instance.scan_thread.is_alive())
                self.assertFalse(instance.keeper.thread.is_alive())
                self.assertFalse(instance.keeper.channels)
            finally:
                instance.controls=None

    # origin: S7 internal review rv-drill F7. The driver's foreground sample
    # must not poison a recoverable restart that the background reader accepts.
    def test_sample_preserves_declared_reader_outage_until_rescan(self):
        with live_fixture() as (instance,_,_,_,_):
            instance.manifest['arm']='C3'
            instance.scan()
            before={name:reader.cursor for name,reader in instance.scans.items()}
            now=time.monotonic_ns()
            instance.fault_interval=(now,now+5_000_000_000)
            original=instance.scan
            def unavailable():
                raise ConnectionRefusedError()
            instance.scan=unavailable
            try:
                instance.snapshot()
                self.assertTrue(instance.outage)
                self.assertFalse(instance.keeper.setup_failures)
                self.assertEqual(before,{name:reader.cursor for name,reader in instance.scans.items()})
            finally:
                instance.scan=original
            instance.drain()
            self.assertIsNone(instance.outage)
            self.assertTrue((instance.keeper.evidence/'reader-recovery.json').exists())

    # origin: S7 internal review rv-drill F1. The final cut is not an original
    # owner join; the fixture continues observing rather than judging early.
    def test_judge_waits_for_original_retirement(self):
        with native_owner_fixture() as instance:
            original = instance.native_results()['ctl1']
            supervised = instance.launcher.children[0][1]
            self.assertIsNone(supervised.poll())
            with instance.keeper.lock:
                instance.keeper.records.events=[e for e in instance.keeper.records.events if e['kind']!='retirement']
            def retire():
                time.sleep(.15)
                with instance.keeper.lock:
                    instance.keeper.records.events.append(event('retirement',5,producer='ctl1',
                        admission_closed=True,retirement_digest='d'*64,stop_nonce='joined'))
            child=threading.Thread(target=retire)
            child.start()
            try:
                started = time.monotonic()
                instance.await_originals()
                self.assertGreaterEqual(time.monotonic()-started, .15)
                self.assertTrue(instance.owners_joined(instance.keeper.records.events,instance.manifest['transfers'][0]))
                self.assertIsNone(supervised.poll())
                for _ in range(2): instance.drains.append(instance.keeper.drain())
                self.assertIsNone(supervised.poll())
                instance.release_originals()
                self.assertEqual(supervised.poll(), original['native_rc'])
                self.assertEqual(load(instance.keeper.evidence/'native-release.json')['results']['ctl1'], original)
            finally:
                child.join(timeout=2)
                self.assertFalse(child.is_alive())
