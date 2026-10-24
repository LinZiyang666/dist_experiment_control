#!/bin/sh
# origin: hot-update S6 plan §10.2 / T58.
# Hermetic stage/dispatch/native-verdict controls. No deployment library is
# sourced by the test: only owned pure functions and the existing assert owner.
set -eu
SIMDIR=${SIMDIR:-$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)}
export SIMDIR
SELECT=${1:-all}
TestBrokerDrillNativeVerdictControls() {
python3 -I -B - "$SIMDIR" "$SELECT" <<'PYTEST'
import ctypes, hashlib, importlib.util, json, os, pathlib, re, runpy, signal, subprocess, sys, tempfile, time, unittest

ROOT = pathlib.Path(sys.argv[1])
LIB = ROOT/'drills/lib/hot_update_broker.sh'
# Even a deliberately broken supervisor is reaped by this hermetic test owner.
if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0) != 0:
    raise RuntimeError('test subreaper setup failed')

def function(name):
    source = LIB.read_text()
    match = re.search(r'^'+name+r'\(\) \{\n.*?^}\n', source, re.M|re.S)
    if match is None:
        raise AssertionError('missing real function '+name)
    return match.group()

def shell(body, directory, timeout=10):
    return subprocess.run(['sh', '-c', body, 'broker-control', str(ROOT)], cwd=directory,
                          capture_output=True, text=True, timeout=timeout)

class BrokerDrillControls(unittest.TestCase):
    def test_every_owned_shell_file_parses_and_whole_library_loads(self):
        for name in ['drills/37-hot-update-broker.sh','drills/39-hot-update-legacy-n1.sh',
                     'drills/75-hot-update-flows.sh','drills/lib/hot_update_broker.sh',
                     'tests/hot-update-broker-drill-test.sh']:
            result=subprocess.run(['sh','-n',str(ROOT/name)],capture_output=True,text=True,timeout=5)
            self.assertEqual(result.returncode,0,result.stderr)
        with tempfile.TemporaryDirectory(prefix='broker-library-') as directory:
            result=shell('HERE="$1"\n. "$HERE/drills/lib/hot_update_broker.sh"\ncommand -v flow_fixture\n',directory)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertEqual(result.stdout.strip(),'flow_fixture')

    # Exact test name from §11. All controls inspect the actual on-disk sources.
    def test_TestBrokerDrillNativeVerdictControls(self):
        d37 = (ROOT/'drills/37-hot-update-broker.sh').read_text()
        d39 = (ROOT/'drills/39-hot-update-legacy-n1.sh').read_text()
        expected = {'arms':'A A1 B T R', 'fixture':'A=install A1=N1 B=N3-live T=N3-live R=N1',
                    'grows':'A=0 A1=0 B=2 T=2 R=0', 'worst':'A=420 A1=420 B=720 T=1200 R=360',
                    'forgoes':'A=- A1=- B=- T=- R=-'}
        # origin: S8 internal review s8-hermetic F5. S8/S9 are additive;
        # every header still pins all historical arms and new wire cells.
        legacy = runpy.run_path(str(ROOT/'tests/testdata/upgrade-legacy-manifest.py'))['MANIFEST39']
        for source, headers in ((d37,expected),(d39,legacy)):
            for key, value in headers.items():
                self.assertEqual(re.findall(r'^# '+key+r': (.*)$',source,re.M),[value])
        body = d37.split('case "${ARM:?}" in')[1]
        self.assertEqual(re.findall(r'^    (A|A1|B|T|R)\)',body,re.M),['A','A1','B','T','R'])
        # Parent join is the actual runner, not a private copy in this package.
        runner = (ROOT/'run-drills.sh').read_text()
        self.assertIn('manifest_arms',runner)
        self.assertNotIn('exit 0',function('broker_native'))
        early = d39.split('case "${ARM:?}" in')[1].split('F)')[0]
        self.assertIn('BR)', early)
        self.assertIn('broker_legacy_arm',early)
        self.assertNotIn('_h0_prepare',early)
        self.assertIn('case "${ARM:-}" in X|Y) drill_install_traps _cleanup ;; esac',d39)
        self.assertIn('broker_legacy_arm grow_to_3', early)
        self.assertIn('"$1" 2 1 0',function('broker_legacy_arm'))
        single = function('broker_prepare').split('single)')[1].split('sole-voter)')[0]
        self.assertNotIn(' init ', single)
        self.assertIn('broker-only',function('broker_roll'))
        self.assertNotIn('node upgrade',function('broker_roll'))
        self.assertIn('colocated_agent_up',function('broker_prepare'))
        self.assertIn("_bos_nodes='brk1 brk2 brk3'",function('broker_observers_start'))
        self.assertIn('for _bow_job in $BROKER_WATCH_JOBS',function('broker_observers_wait'))
        self.assertIn('legacy-follower-home-prior',function('broker_legacy_arm'))
        self.assertIn('UNPINNED',function('broker_legacy_arm'))
        self.assertIn('transfer_run T',function('broker_transfer_lane'))
        self.assertIn('transfer_cleanup',function('broker_transfer_lane'))
        self.assertIn('broker_fault held',body)
        self.assertIn('broker_fault active',body)
        self.assertIn('fault-active-release',function('broker_fault'))
        self.assertIn('fault-current-witness',function('broker_fault'))
        self.assertIn('bundle-judge',function('broker_finish'))
        self.assertIn('--flow-fixture-root "$FLOW_EV"',function('broker_finish'))
        self.assertNotIn('flow_fixture cleanup',function('broker_finish'))
        self.assertIn('install -m 0755 "/proc/$p/exe" /usr/local/bin/tether.prev',function('broker_stage'))
        self.assertIn('. "$HERE/drills/lib/flow_fixture"',(ROOT/'drills/75-hot-update-flows.sh').read_text())

    def test_native_statuses_and_join_keep_all_children(self):
        spec = importlib.util.spec_from_file_location('broker_probe',ROOT/'drills/lib/hot_update_broker_probe.py')
        probe = importlib.util.module_from_spec(spec); spec.loader.exec_module(probe)
        with tempfile.TemporaryDirectory(prefix='broker-native-') as directory:
            for rc, verdict in ((0,'GREEN'),(1,'ASSERT-FAIL'),(2,'SETUP-RED'),(3,'PRODUCT-RED'),(4,'INCOMPLETE'),(9,'SETUP-RED')):
                script = '. "$1/lib/assert.sh"\nlog() { :; }; ok() { :; }; err() { :; }\nINSTANCE=drill-native-control\ndrill_begin native\n'
                result = shell(script+function('broker_native')+f'broker_native {rc}\ndrill_end\n',directory)
                self.assertEqual(result.returncode, {'GREEN':0,'ASSERT-FAIL':1,'SETUP-RED':2,'PRODUCT-RED':3,'INCOMPLETE':4}[verdict])
                self.assertIn('verdict='+verdict+' ',result.stderr)
            green = dict(rc=0,verdict='GREEN',failures=[],gaps=[])
            for ordinal in range(5):
                children = [dict(green) for _ in range(5)]
                children[ordinal] = dict(rc=4,verdict='INCOMPLETE',failures=[],gaps=['missing_arm'])
                self.assertEqual(probe.join_children(children)['rc'],4)

    def test_every_declared_arm_executes_and_finishes_its_own_cohort(self):
        source=(ROOT/'drills/37-hot-update-broker.sh').read_text()
        dispatch='case "${ARM:?}" in'+source.split('case "${ARM:?}" in')[1].split('\nesac\n')[0]+'\nesac\n'
        expected={'A':['prepare single','reload forward','finish'],
                  'A1':['prepare sole-voter','supply sole-voter-plan','reload forward','supply sole-voter-write-stall','finish'],
                  'B':['prepare quorum','supply auto-rebalance-prior','roll','finish'],
                  'T':['prepare quorum','transfer quorum','finish','cleanup','prepare single','transfer single','finish'],
                  'R':['prepare single','fault held','fault active','finish']}
        doubles='SIM_VENDOR=/synthetic\nbroker_prepare() { printf "prepare %s\\n" "$1"; }\nbroker_reload() { printf "reload %s\\n" "$1"; }\nbroker_supply() { printf "supply %s\\n" "$1"; }\nbroker_roll() { printf "roll\\n"; }\nbroker_transfer_lane() { printf "transfer %s\\n" "$1"; }\nbroker_fault() { printf "fault %s\\n" "$1"; }\nbroker_finish() { printf "finish\\n"; }\nbroker_cleanup() { printf "cleanup\\n"; }\n'
        with tempfile.TemporaryDirectory(prefix='broker-dispatch-') as directory:
            for arm,events in expected.items():
                result=shell('ARM='+arm+'\n'+doubles+dispatch,directory)
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertEqual(result.stdout.splitlines(),events,arm)

    def test_legacy_native_loss_and_unpinned_or_mismatched_loss_remain_red(self):
        with tempfile.TemporaryDirectory(prefix='broker-legacy-native-') as directory:
            raw=pathlib.Path(directory)/'signature.out'
            prefix='. "$1/lib/assert.sh"\nlog() { :; }; ok() { :; }; err() { :; }\nINSTANCE=drill-legacy-control\nSIM_EVIDENCE_DIR="$PWD"\ndrill_begin legacy\nBROKER_LEGACY_REGEX="^synthetic reviewed loss$"\n'
            # origin: S8 internal review s8-hermetic F5. Signed exec is a
            # required independent prerequisite, backed by real-shaped inputs.
            pin=pathlib.Path(directory)/'pin.json'
            pin.write_text(json.dumps(dict(source_sha='a'*40,binary_sha256='b'*64)))
            identity=dict(pid=4200,start_ticks=42000,boot_id='synthetic-boot')
            evidence=dict(command_rc=0,source_sha='a'*40,signed_command=True,
                          old_running_sha='b'*64,new_running_sha='c'*64,role='follower',
                          topology='N3',run_unwind_observed=True,before=identity,after=identity)
            (pathlib.Path(directory)/'legacy-exec.json').write_text(json.dumps(evidence))
            prefix+='BROKER_EV="$PWD"; BROKER_LEGACY_PIN="$PWD/pin.json"; BROKER_SHA='+('c'*64)+'; BROKER_COMMAND_RC=0\n'
            functions=function('broker_native')+function('broker_legacy_observation')+function('broker_legacy_exec_required')+function('broker_legacy_verdict')
            for rc,observation,want in [(1,'synthetic reviewed loss\n',3),(0,'',1),(2,'synthetic reviewed loss\n',2),
                                       (4,'',4),(1,'unreviewed failure\n',1),(1,'diagnostic\nsynthetic reviewed loss\n',1)]:
                raw.write_text(observation)
                result=shell(prefix+functions+f'broker_legacy_verdict {rc} "$PWD/signature.out"\ndrill_end\n',directory)
                self.assertEqual(result.returncode,want,result.stderr)

    def test_command_wait_preserves_child_rc_and_custody(self):
        with tempfile.TemporaryDirectory(prefix='broker-child-') as directory:
            script = 'BROKER_EV="$PWD"; BROKER_COMMAND_JOB=\n'+function('broker_child_run')+function('broker_command_start')+function('broker_command_wait')
            for rc in (0,1,2,3,4):
                result = shell(script+f'broker_command_start child sh -c "exit {rc}"\npid=$BROKER_COMMAND_JOB\nbroker_command_wait\nrc=$?\ntest "$rc" = {rc} || exit 99\ntest -z "$BROKER_COMMAND_JOB" || exit 98\n! kill -0 "$pid" 2>/dev/null\n',directory)
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertEqual((pathlib.Path(directory)/'command-child.rc').read_text(),str(rc)+'\n')

    def test_setup_aborts_before_provision_and_missing_fixture(self):
        with tempfile.TemporaryDirectory(prefix='broker-prior-') as directory:
            script = function('broker_require_fixture')+'\nHERE="$1"; BROKER_LIVE_SUPPLIER=/missing/supplier\nflow_fixture() { return 0; }\nbroker_require_fixture\n'
            self.assertEqual(shell(script,directory).returncode,2)
            prepare = function('broker_prepare')
            self.assertLess(prepare.index('broker_require_fixture'),prepare.index('drill_install_traps'))
            self.assertLess(prepare.index('broker_require_fixture'),prepare.index('"$SIM" nuke'))

    # origin: S10 internal review RUNALL-A F4 / hot-update S10 plan §6.
    def test_supervisor_reaps_cancelled_and_detached_descendants(self):
        with tempfile.TemporaryDirectory(prefix='broker-descendant-') as directory:
            for round in range(5):
                root=pathlib.Path(directory)
                pidfile=root/f'child-{round}.pid'
                worker=root/'worker.py'
                # Grandchild deliberately creates a different session. Only
                # subreaper custody can reap it after its parent is terminated.
                worker.write_text('import os,pathlib,subprocess,sys,time\nchild=subprocess.Popen([sys.executable,"-c","import time; time.sleep(30)"],start_new_session=True)\npathlib.Path(sys.argv[1]).write_text(str(os.getpid())+" "+str(child.pid))\ntime.sleep(30)\n')
                script=function('broker_child_run')+f'broker_child_run "{root}/child.out" "{root}/child.rc" python3 -I -B "{worker}" "{pidfile}"\n'
                job=subprocess.Popen(['sh','-c',script],cwd=directory,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                pids=[]
                try:
                    until=time.monotonic()+5
                    while not pidfile.exists() and job.poll() is None and time.monotonic()<until:
                        time.sleep(.01)
                    self.assertTrue(pidfile.exists(),'supervisor never started real children')
                    pids=[int(value) for value in pidfile.read_text().split()]
                    job.send_signal(signal.SIGTERM)
                    stdout,stderr=job.communicate(timeout=8)
                    # This worker abandons its setsid child. Emergency reap
                    # preserves125 even when the resulting census is empty.
                    self.assertEqual(job.returncode,125,stderr.decode())
                    self.assertEqual((root/'child.rc').read_text(),'125\n')
                    self.assertIn('surviving descendants',(root/'child.out').read_text())
                    for pid in pids:
                        self.assertFalse(pathlib.Path('/proc/'+str(pid)).exists(),'unreaped owned descendant')
                finally:
                    if job.poll() is None:
                        job.kill();job.communicate()
                    # A broken supervisor mutation must not leak its children.
                    for pid in pids:
                        try: os.kill(pid,signal.SIGKILL)
                        except ProcessLookupError: pass
                    for pid in pids:
                        try: os.waitpid(pid,0)
                        except ChildProcessError: pass

    def test_held_and_active_overlays_reach_real_import_barriers(self):
        with tempfile.TemporaryDirectory(prefix='broker-overlay-') as directory:
            repo = ROOT.parent.parent
            original = repo/'internal/brokerimage/import.go'; before=original.read_bytes()
            for point in ('held','active'):
                out=pathlib.Path(directory)/point
                result=shell(function('broker_fault_overlay')+f'broker_fault_overlay "{repo}" "{out}" {point} /tmp/tether-37-'+'a'*32+'\n',directory)
                self.assertEqual(result.returncode,0,result.stderr)
                source=(out/'import.go').read_text()
                mapping=json.loads((out/'overlay.json').read_text())['Replace']
                self.assertEqual(list(mapping),[str(original.resolve())])
                self.assertNotIn('os.Getenv',source)
                self.assertNotIn('broker-image-v2',source)
                if point=='held':
                    start=source.index('file, err := s.source.TakeFile(ref)')
                    fault=source.index('len(s.taken) == 1',start)
                    self.assertLess(start,fault)
                    self.assertLess(fault,source.index('s.taken[key] = file',start))
                    self.assertIn('nonnil_file',source)
                    self.assertIn('file != nil && len(s.taken) == 1',source)
                else:
                    active=source.index('i.active = true')
                    business=source.index('i.control.StartBusiness(deadline)')
                    barrier=source.index('// origin: hot-update S6 plan §10.2 / T58; temporary ACTIVE overlay.')
                    self.assertLess(active,business); self.assertLess(business,barrier)
                    self.assertIn('for deadlineAlive(deadline)',source)
                    self.assertIn('fault-active-release',source)
                    self.assertIn('return errors.Join(ErrActive, ErrImage)',source)
            self.assertEqual(original.read_bytes(),before)

    def test_legacy_pin_binds_literal_signature_artifact_and_reviewed_receipt(self):
        with tempfile.TemporaryDirectory(prefix='broker-pin-') as directory:
            root=pathlib.Path(directory); binary=root/'previous'; binary.write_bytes(b'hermetic old supply')
            source=dict(tag='v1.2.3',commit='a'*40)
            (root/'source.json').write_text(json.dumps(source))
            signature='synthetic literal EOF|RST observation (no deployment claim)'
            receipt=json.dumps(dict(observed_signature=signature)).encode()
            (root/'receipt.json').write_bytes(receipt)
            pin=dict(schema='broker-legacy-pin-v1',reviewed_by='lead',tag=source['tag'],source_sha=source['commit'],
                     binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                     preflight_capability=dict(signed_reload=True,n3=True,legacy_run_unwind=True),
                     observed_signature=signature,receipt_path='receipt.json',receipt_sha256=hashlib.sha256(receipt).hexdigest())
            def judge(value):
                (root/'pin.json').write_text(json.dumps(value))
                return shell(function('broker_legacy_pin_ok')+'broker_legacy_pin_ok "$PWD/pin.json" "$PWD/previous" "$PWD/source.json"\n',directory)
            good=judge(pin); self.assertEqual(good.returncode,0,good.stderr)
            self.assertEqual(good.stdout.strip(),'^'+re.escape(signature)+'$')
            for field,value in [('reviewed_by','predicted'),('source_sha','b'*40),('binary_sha256','c'*64),
                                ('preflight_capability',dict(signed_reload=True,n3=False,legacy_run_unwind=True)),
                                ('receipt_path','../receipt.json'),('receipt_sha256','d'*64),('observed_signature','guessed EOF')]:
                broken=dict(pin); broken[field]=value
                self.assertEqual(judge(broken).returncode,2,field)
            (root/'receipt.json').write_text('tampered')
            self.assertEqual(judge(pin).returncode,2)

    def test_cleanup_waits_every_started_process_and_fixture(self):
        cleanup=function('broker_cleanup')
        for owner in ('BROKER_COMMAND_JOB','BROKER_WATCH_JOB','BROKER_PROBE_JOB','BROKER_FAULT_JOB'):
            self.assertIn('"$'+owner+'"',cleanup)
        self.assertIn('wait "$_bc_job"',cleanup)
        self.assertIn('flow_fixture cleanup',cleanup)
        with tempfile.TemporaryDirectory(prefix='broker-cleanup-') as directory:
            # Five real process rounds: shell owns the wait, no daemon escapes.
            for round in range(5):
                script=cleanup+'\nflow_fixture() { printf fixture >> "$PWD/cleanup"; }\n'
                script+='BROKER_WATCH_JOBS=\nsleep 30 & BROKER_COMMAND_JOB=$!\nsleep 30 & BROKER_WATCH_JOB=$!\nsleep 30 & BROKER_PROBE_JOB=$!\nsleep 30 & BROKER_FAULT_JOB=$!\n'
                script+='pids="$BROKER_COMMAND_JOB $BROKER_WATCH_JOB $BROKER_PROBE_JOB $BROKER_FAULT_JOB"\nbroker_cleanup\nfor pid in $pids; do ! kill -0 "$pid" 2>/dev/null || exit 99; done\n'
                self.assertEqual(shell(script,directory).returncode,0)
            self.assertEqual((pathlib.Path(directory)/'cleanup').read_text(),'fixture'*5)

# Each registered arm runs the real owner controls for its dispatch, setup and
# native classification seam. Unknown selectors must fail rather than run zero
# tests and give the section-reconciliation gate a vacuous green.
# origin: hot-update S6 plan §13 / T58
common = ['test_TestBrokerDrillNativeVerdictControls',
          'test_every_declared_arm_executes_and_finishes_its_own_cohort',
          'test_native_statuses_and_join_keep_all_children']
arm_tests = {
    'A': common + ['test_setup_aborts_before_provision_and_missing_fixture'],
    'A1': common + ['test_command_wait_preserves_child_rc_and_custody'],
    'B': common + ['test_command_wait_preserves_child_rc_and_custody'],
    'T': common + ['test_setup_aborts_before_provision_and_missing_fixture'],
    'R': common + ['test_held_and_active_overlays_reach_real_import_barriers'],
    'BR': ['test_TestBrokerDrillNativeVerdictControls',
           'test_legacy_native_loss_and_unpinned_or_mismatched_loss_remain_red',
           'test_legacy_pin_binds_literal_signature_artifact_and_reviewed_receipt'],
}
selector = sys.argv[2]
if selector == 'all':
    sys.argv=[sys.argv[0]]
else:
    if selector not in arm_tests:
        raise AssertionError('unknown broker arm control selector')
    sys.argv=[sys.argv[0]]+['BrokerDrillControls.'+name for name in arm_tests[selector]]
try:
    unittest.main(verbosity=2)
finally:
    # A wait/cancellation mutant can orphan its supervisor to this subreaper.
    # Failed assertions still leave no owned process for the next package.
    for value in pathlib.Path('/proc/self/task/'+str(os.getpid())+'/children').read_text().split():
        try: os.kill(int(value),signal.SIGTERM)
        except ProcessLookupError: pass
    until=time.monotonic()+3
    while True:
        try: pid,_=os.waitpid(-1,os.WNOHANG)
        except ChildProcessError: break
        if pid: continue
        if time.monotonic()>=until:
            for value in pathlib.Path('/proc/self/task/'+str(os.getpid())+'/children').read_text().split():
                try: os.kill(int(value),signal.SIGKILL)
                except ProcessLookupError: pass
        time.sleep(.01)
PYTEST
}
TestBrokerDrillNativeVerdictControls
