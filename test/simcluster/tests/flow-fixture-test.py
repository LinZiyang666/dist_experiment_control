#!/usr/bin/env python3
"""Live collector controls; every external product command is a process fake.

origin: hot-update S5 plan §9.1 / T48. These are collector tests, never deploy
receipts. The local duplex exercise runs the actual container peer unchanged.
"""
import copy
import contextlib
import fcntl
import importlib.util
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
LIB = ROOT / "drills/lib"
spec = importlib.util.spec_from_file_location("fixture", LIB / "flow_fixture.py")
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
peer = fixture.peer
NONCE = "ab"*16

# origin: hot-update S10 plan §6 / P11-P12 seam. A stale mock must fail at
# the process boundary before a hermetic test can resolve the installed CLI.
_start_supervised = peer.start_supervised
def hermetic_start(argv, **options):
    if Path(argv[0]).name == 'tether':
        raise AssertionError('installed product execution forbidden in fixture selftests')
    return _start_supervised(argv, **options)
peer.start_supervised = hermetic_start

# The shim records only public argv. Subscription input is never logged. The
# controlled syscall/log receipts stand for the container boundary, not for a
# second implementation of the collector's assertions.
SHIM = r'''
import json, pathlib, sys
root=pathlib.Path(sys.argv[1]); argv=sys.argv[2:]
with (root/'calls').open('a') as out: out.write(json.dumps(argv)+'\n')
control=json.loads((root/'control.json').read_text())
if control.get('native_rc'):
    print(control.get('native_error',''),file=sys.stderr)
    sys.exit(control['native_rc'])
op=argv[0]
if op=='logs':
    stream=argv[1]; raw=control.get(stream,'').encode()
    pathlib.Path(argv[3]).write_bytes(raw)
    pathlib.Path(argv[4]).write_text(json.dumps(dict(read_ok=True,inode=11,length=len(raw))))
elif op=='input':
    if 'curl' in argv:
        sys.stdin.read(); print('private supply'); sys.exit(0)
    if 'key' in argv:
        sys.stdin.buffer.read()
        print(json.dumps(dict(host='brk1',port=14500)))
        sys.exit(0)
    request=json.load(sys.stdin)
    with (root/'commands').open('a') as out: out.write(request['command']+'\n')
    if request['command']=='cleanup': (root/('stop-'+argv[1])).touch()
    value=control.get(request['command'],{})
    print(json.dumps(value))
    if request['command']=='judge': sys.exit(value.get('rc',0))
elif op=='exec' and 'kernel' in argv:
    print(json.dumps(control['kernel']))
elif op=='exec' and 'sqlite3' in argv:
    print(json.dumps(control.get('sql',[])))
elif op=='exec' and argv[2]=='cat':
    print(json.dumps(dict(port=9001,open_port=9002,identity=dict(pid=100,boot_id='boot',start_ticks=100))))
elif op=='observe-start': print(json.dumps(dict(cipher_pressure_loopback_mtu=1500)))
elif op=='observe-stop': pass
elif op=='ip': print(control.get('agent_ip','127.0.0.1'))
elif op=='exec-user' or op=='exec' and 'serve' in argv:
    import time
    if 'hot_update_mixed_probe.py' in ' '.join(argv) and 'run' in argv:
        while not (root/'request-finished').exists(): time.sleep(.01)
    else:
        while not (root/('stop-'+argv[1])).exists(): time.sleep(.01)
elif op=='exec' and 'hot_update_mixed_probe.py' in ' '.join(argv) and 'mark' in argv:
    (root/'request-finished').touch()
elif op=='ctl' and 'explain' in argv:
    print(json.dumps(dict(public_port=14001,home_broker='brk1')))
elif op=='ctl' and 'create' in argv: print('https://brk1/sub/fake-token')
elif op=='ctl' and argv[1:]==['proxy','status','--json']:
    print(json.dumps(control.get('proxy_status',dict(enabled=True,nodes=[dict(nid='agt1',ready=True)]))))
elif op=='copy-to' or op=='copy-from' or op=='exec' or op=='ctl' or op=='policy': pass
else: sys.exit(2)
'''


def kernel():
    return dict(identity=dict(pid=40, boot_id="boot", start_ticks=40), uid=1000, at=10,
                running_sha="a"*64, marker=None, fds=[], connections=[])


class ArtifactGateTests(unittest.TestCase):
    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
    def test_artifact_withholds_completion_until_original_open_prior(self):
        gate = fixture.load('flow_artifact_gate')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = b'unchanged signed artifact' * 10000
            source, destination = io.BytesIO(data), io.BytesIO()
            errors = []
            def run():
                try: gate.copy_gated(source, destination, root)
                except BaseException as error: errors.append(error)
            thread = threading.Thread(target=run)
            thread.start()
            try:
                deadline = time.monotonic()+2
                while not (root/'waiting').exists() and time.monotonic()<deadline:
                    time.sleep(.001)
                self.assertTrue((root/'waiting').exists())
                thread.join(timeout=.05)
                self.assertEqual(destination.getvalue(), data[:-1])
                self.assertTrue(thread.is_alive())
            finally:
                (root/'release').touch()
                thread.join(timeout=2)
            self.assertFalse(errors)
            self.assertFalse(thread.is_alive())
            self.assertEqual(destination.getvalue(), data)


class OpenSynFaultTests(unittest.TestCase):
    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.A
    def test_open_waits_for_new_native_executable_staging(self):
        script = fixture.load('flow_open_fault')
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            old = root/'.tether-upgrade-old'/'tether'; old.parent.mkdir()
            old.write_bytes(b'old'); old.chmod(0o755)
            current = root/'.tether-upgrade-current'/'tether'
            released = threading.Event()
            results, errors = [], []
            @contextlib.contextmanager
            def release(request, timeout):
                self.assertEqual(request.method, 'POST')
                self.assertEqual(timeout, 1)
                current.parent.mkdir(); current.write_bytes(b'new'); current.chmod(0o600)
                released.set()
                yield SimpleNamespace(status=204)
            def wait():
                try: results.append(script.wait_staged('https://artifact', root))
                except BaseException as error: errors.append(error)
            with mock.patch.object(script.urllib.request, 'urlopen', side_effect=release):
                thread = threading.Thread(target=wait); thread.start()
                try:
                    self.assertTrue(released.wait(1))
                    thread.join(.05)
                    self.assertTrue(thread.is_alive(), 'old or incomplete artifact started the OPEN')
                    current.chmod(0o755)
                    thread.join(2)
                    self.assertFalse(thread.is_alive())
                    self.assertFalse(errors)
                    self.assertEqual(results[0]['ino'], current.stat().st_ino)
                finally:
                    current.chmod(0o755); thread.join(6)

    # origin: hot-update S5 plan §9.2 / T48 / DRILL-RUN
    def test_first_commit_fault_requires_actual_candidate_and_bounded_syn(self):
        script = fixture.load('flow_open_fault')
        for defect in (None, 'unpersisted', 'root', 'wrong-image', 'writable', 'expired'):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                marker = root/'marker.json'
                marker.write_text(json.dumps(dict(state='pending', boot_count=0 if defect=='unpersisted' else 1,
                    new_sha='wrong' if defect=='wrong-image' else peer.hashlib.sha256(b'artifact').hexdigest())))
                process = root/'proc'/'123';process.mkdir(parents=True)
                (process/'status').write_text('Uid: '+('0' if defect=='root' else '1000')+' 1000 1000 1000')
                (process/'exe').write_bytes(b'artifact')
                binary = mock.Mock()
                binary.stat.return_value = SimpleNamespace(st_uid=1000, st_mode=0o755)
                mapping = {'/home/sim/.local/bin/.tether-upgrade.json': marker,
                           '/proc': root/'proc', '/home/sim/.local/bin': binary}
                queue = mock.Mock()
                def accept(ids):
                    self.assertEqual(ids, [7])
                    binary.chmod.assert_called_once_with(0o555)
                queue.accept.side_effect = accept
                with mock.patch.object(script, 'Path', side_effect=lambda value: mapping[value]), \
                     mock.patch.object(script.subprocess, 'check_output', return_value=b'123'), \
                     mock.patch.object(script.subprocess, 'call', return_value=0 if defect=='writable' else 1), \
                     mock.patch.object(script.subprocess, 'run'):
                    started = time.monotonic()-(3 if defect=='expired' else 0)
                    if defect:
                        with self.assertRaises((ValueError, TimeoutError)):
                            script.fail_first_commit(root, queue, [7], started)
                        queue.accept.assert_not_called()
                    else:
                        script.fail_first_commit(root, queue, [7], started)
                        queue.accept.assert_called_once_with([7])
                        result = json.loads((root/'watchdog-released.json').read_text())
                        self.assertLessEqual(result['nats_block_ms'], 2000)
                        self.assertEqual(result['mode'], '755')

    # Synthetic cut declarations drive the actual watcher function; the Docker
    # netns selftest separately proves kernel queue retention of the original FD.
    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
    def exercise(self, original_syn=True):
        script=fixture.load('flow_open_fault')
        records=mock.Mock(events=[dict(kind='final-cut',at=111)])
        queue=mock.Mock()
        queue.receive.return_value=[7]
        fetches=[]
        def fetch(*_):
            fetches.append(True)
            if len(fetches)==3:
                records.events.append(dict(kind='final-cut',at=222))
        selects=[]
        def ready(*_):
            selects.append(True)
            return ([queue.socket] if original_syn and len(selects)==1 else [],[],[])
        with tempfile.TemporaryDirectory(prefix='s5-syn-cut-') as directory:
            root=Path(directory)
            with mock.patch.object(script,'Queue',return_value=queue), \
                 mock.patch.object(script.observation,'Records',return_value=records), \
                 mock.patch.object(script.observation,'fetch',side_effect=fetch), \
                 mock.patch.object(script.select,'select',side_effect=ready), \
                 mock.patch.object(script.subprocess,'run') as run:
                if original_syn:
                    script.watch(root,75)
                    queue.accept.assert_called_once_with([7])
                    self.assertEqual(len(fetches),3)
                    receipt=json.loads((root/'open-fault-released.json').read_text())
                    self.assertEqual(receipt['cut_boot_ns'],222)
                    self.assertEqual(receipt['original_packets'],1)
                    run.assert_called_once_with(['iptables','-F','FLOWOPEN'],check=True)
                else:
                    with self.assertRaisesRegex(ValueError,'cut_without_original_syn'):
                        script.watch(root,75)
                    queue.accept.assert_not_called()
                    self.assertFalse((root/'open-fault-released.json').exists())
                records.close.assert_called_once()
                queue.close.assert_called_once()

    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
    def test_local_prior_requires_same_pending_inode_and_no_target_accept(self):
        script=fixture.load('flow_open_fault')
        event=dict(at=123,endpoint=bytes([1])*16,flow=bytes([2])*16,values=[0]*16)
        event['values'][3:7]=[2,1,1,77];event['values'][14]=1
        records=SimpleNamespace(live_flows=lambda: {event['flow']:event},partial=lambda: (None,dict(n=1)))
        census=dict(connections=[dict(state='02',ino=77)])
        target=dict(flows={})
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            with mock.patch.dict(sys.modules,{'flow_fixture_peer':peer}), \
                 mock.patch.object(script.subprocess,'check_output',return_value=b'123'), \
                 mock.patch.object(peer,'kernel',return_value=census), \
                 mock.patch.object(peer,'call',return_value=target):
                self.assertTrue(script.capture_open_prior(records,root,root))
                self.assertEqual(json.loads((root/'open-prior.json').read_text())['flow'],event['flow'].hex())
                census['connections'][0]['ino']=78
                self.assertFalse(script.capture_open_prior(records,root,root))
                census['connections'][0]['ino']=77
                target['flows']['opening']=dict(target_accepts=1)
                self.assertFalse(script.capture_open_prior(records,root,root))

    def test_queued_original_syn_releases_only_at_a_new_native_cut(self):
        self.exercise()

    def test_native_cut_without_original_syn_cannot_sign_fault_release(self):
        self.exercise(False)



class CapacityPhaseTests(unittest.TestCase):
    # origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN.
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.collector = fixture.Fixture(SimpleNamespace(root=self.root, nonce=NONCE,
            profile="capacity-32", arm="C"), mock.Mock())
        self.specs, self.raw, self.before, self.final = [], {}, {}, {}
        for role in ('client','target'):
            self.before[role] = dict(flows={})
            self.final[role] = dict(flows={})
        for n in range(32):
            name = 'saturation-'+str(n)
            spec = dict(id=name, up=peer.declaration(name+'.up',NONCE,21,16),
                        down=peer.declaration(name+'.down',NONCE,21,16))
            self.specs.append(spec)
            for role,direction in (('client','up'),('target','down')):
                self.raw[role,name,'sent'] = b''.join(fixture.probe.frame(NONCE,k,peer.payload(name+'.'+direction,k,16)) for k in range(21))
            for role,other in (('client','target'),('target','client')):
                self.raw[role,name,'received'] = self.raw[other,name,'sent']
                identity = dict(pid=10,boot_id='boot',start_ticks=20,dev=1,ino=n+100,
                                type=0o140000,tuple=['tcp','local','remote'],app_nonce=NONCE)
                row = dict(target_accepts=1,fin_at=25,events=[dict(kind='eof',at=26)],identities=[identity,identity],
                    sent=[dict(v,at=v['seq']+1) for v in fixture.probe.parse_frames(self.raw[role,name,'sent'],NONCE)],
                    received=[dict(v,at=v['seq']+1) for v in fixture.probe.parse_frames(self.raw[role,name,'received'],NONCE)])
                self.before[role]['flows'][name] = copy.deepcopy(row)
                self.final[role]['flows'][name] = copy.deepcopy(row)

    def write_raw(self, folder):
        for role in ('client','target'):
            (folder/role).mkdir(parents=True,exist_ok=True)
        for (role,name,suffix),raw in self.raw.items():
            (folder/role/(name+'.'+suffix)).write_bytes(raw)

    def test_saturation_requires_complete_original_ledgers_and_natural_fin(self):
        folder = self.root/'saturation'
        self.write_raw(folder)
        self.collector.check_saturation_ledgers(folder,self.specs,self.before,self.final)
        self.assertTrue((folder/'ledgers.json').exists())
        cases = [('cohort',lambda specs,rows: specs.pop()),
                 ('fin',lambda specs,rows: rows['client']['flows']['saturation-0'].update(fin_at=None)),
                 ('early-fin',lambda specs,rows: rows['client']['flows']['saturation-0'].update(fin_at=1)),
                 ('early-eof',lambda specs,rows: rows['target']['flows']['saturation-0'].update(events=[dict(kind='eof',at=1)])),
                 ('reset',lambda specs,rows: rows['target']['flows']['saturation-0'].update(events=[dict(kind='rst',at=26)])),
                 ('accept',lambda specs,rows: rows['target']['flows']['saturation-0'].update(target_accepts=2)),
                 ('identity',lambda specs,rows: rows['client']['flows']['saturation-0']['identities'][1].update(ino=9999)),
                 ('sender',lambda specs,rows: rows['client']['flows']['saturation-0']['sent'].pop()),
                 ('sender-seq',lambda specs,rows: rows['client']['flows']['saturation-0']['sent'][0].update(seq=19)),
                 ('receiver',lambda specs,rows: rows['target']['flows']['saturation-0']['received'].pop()),
                 ('receiver-seq',lambda specs,rows: rows['target']['flows']['saturation-0']['received'][0].update(seq=19))]
        for name,change in cases:
            with self.subTest(name=name):
                specs,rows = copy.deepcopy(self.specs),copy.deepcopy(self.final)
                change(specs,rows)
                with self.assertRaises((fixture.SetupError,fixture.probe.ContinuityError)):
                    self.collector.check_saturation_ledgers(folder,specs,self.before,rows)
        target = folder/'target/saturation-0.received'
        original = target.read_bytes()
        for raw in (original[:-1],original+original[:100],bytes([original[0]^1])+original[1:]):
            target.write_bytes(raw)
            with self.assertRaises((fixture.SetupError,fixture.probe.ContinuityError)):
                self.collector.check_saturation_ledgers(folder,self.specs,self.before,self.final)
        target.write_bytes(original)

    def exercise_phase(self, defect=None):
        collector = self.collector
        collector.phase = 'prepared'
        collector.saturation_specs = copy.deepcopy(self.specs)
        collector.specs = [dict(id='tcp-0',host='brk1',port=14001)]
        data = hashlib.sha256((NONCE+'capacity-recovery').encode()).hexdigest()
        collector.short_flows = {'capacity-recovery':data}
        source = kernel()
        allocation = [dict(home='brk3',epoch=1)]
        calls,execs = [],[]
        pending = dict(identity=self.final['client']['flows']['saturation-0']['identities'][0])
        def remote(node,command,**fields):
            calls.append(command)
            if command == 'start': return dict(started=True)
            if command == 'snapshot': return dict(self.final['target' if node=='agt1' else 'client'],short_accepts={'capacity-recovery':1})
            if command == 'capacity-open': return pending
            if command == 'capacity-exec':
                execs.append(command)
                if len(execs)<=2:
                    return dict(native_rc=70,error_type='generic' if defect=='type' else 'workload.ErrBackpressure',success=False)
                return dict(native_rc=0,success=defect!='recovery')
            if command == 'capacity-exec-ledger':
                return dict(lines=[NONCE+'-capacity'] if len(execs)>2 or defect=='side-effect' else [])
            if command == 'drain-saturation': return self.final['target' if node=='agt1' else 'client']
            if command == 'capacity-recover': return dict(identity=pending['identity'],bytes=32,sha256=hashlib.sha256(bytes.fromhex(data)).hexdigest())
            if command == 'retire-saturation': return dict(retired=32)
            self.fail(command)
        def product(label,*args):
            calls.append(label)
            if label == 'capacity-full': return dict(custody_complete=True,active_flows=31 if defect=='full-count' else 32,reservation=224<<20 if defect=='full-budget' else 256<<20)
            if label == 'capacity-refused': return dict(custody_complete=True,active_flows=32,reservation=256<<20,flow_refusals=0 if defect=='flow-refusal' else 1,reason=14 if defect=='flow-reason' else 10)
            if label == 'capacity-released': return dict(custody_complete=True,reservation=8<<20 if defect=='unreleased' else 0,settled_originals=31 if defect=='settled-count' else 32)
            self.fail(label)
        def copy_raw(command,*args,**kwargs):
            if command=='copy-from':
                node,source,destination = args
                name,suffix = Path(source).name.rsplit('.',1)
                role = 'target' if node=='agt1' else 'client'
                destination.write_bytes(self.raw[role,name,suffix] if suffix!='json' else b'{}')
        with mock.patch.object(collector,'remote_call',side_effect=remote),mock.patch.object(collector,'product_receipt',side_effect=product), \
             mock.patch.object(collector,'kernel',side_effect=[source,source,source,source,dict(source,running_sha='b'*64) if defect=='source' else source]), \
             mock.patch.object(collector,'sql',side_effect=[allocation,[] if defect=='allocation' else allocation]), \
             mock.patch.object(collector,'checkpoint',side_effect=lambda label:calls.append('checkpoint-'+label)), \
             mock.patch.object(collector.transport,'run',side_effect=copy_raw):
            result = collector.saturation()
        self.assertTrue(result['saturation_complete'])
        self.assertTrue(collector.capacity_complete)
        self.assertLess(calls.index('capacity-full'),calls.index('capacity-open'))
        self.assertLess(calls.index('capacity-refused'),calls.index('drain-saturation'))
        self.assertLess(calls.index('capacity-released'),calls.index('retire-saturation'))
        self.assertEqual(execs,['capacity-exec']*3)
        self.assertEqual(calls.count('start'),1)

    def test_two_phases_require_native_capacity_release_and_same_cluster(self):
        self.exercise_phase()
        for defect in ('type','side-effect','full-count','full-budget','flow-refusal','flow-reason','unreleased','settled-count','recovery','source','allocation'):
            with self.subTest(defect=defect):
                (self.root/'saturation').rename(self.root/('previous-'+defect))
                with self.assertRaises(fixture.SetupError): self.exercise_phase(defect)
                # Early guards can fail before creating the capture directory.
                if not (self.root/'saturation').exists(): (self.root/'saturation').mkdir()

    def test_normal_profile_keeps_shared_budget_headroom(self):
        records=peer.observation.Records()
        owner=SimpleNamespace(nonce=NONCE,observations=records,observation_checkpoints={})
        domains=dict(exec=dict(reservation=160<<20),pty=dict(reservation=0),flow=dict(reservation=64<<20))
        with mock.patch.object(peer.observation,'fetch'),mock.patch.object(records,'ready'),mock.patch.object(records,'domains',return_value=domains):
            result=peer.observation.product_receipt(owner,'baseline',dict(profile='capacity-32',kernel={}))
            self.assertEqual(sum(row['reservation'] for row in result['domains'].values()),224<<20)
            domains['exec']['reservation']+=1
            with self.assertRaisesRegex(peer.observation.CustodyError,'normal_capacity_headroom_missing'):
                peer.observation.product_receipt(owner,'baseline',dict(profile='capacity-32',kernel={}))

    def test_capacity_observation_requires_original_active_and_settled_owners(self):
        observation = peer.observation
        records = observation.Records()
        owner = SimpleNamespace(nonce=NONCE, observations=records, observation_checkpoints={},
            capacity_identity=dict(tuple=['tcp','pending-local','pending-remote']))
        target = copy.deepcopy(self.final['target'])
        for index,(name,row) in enumerate(target['flows'].items()):
            identity = row['identities'][0]
            identity['tuple'] = ['tcp',str(index),str(index+100)]
            row['identities'][1] = copy.deepcopy(identity)
            flow = bytes([index+1])*16
            socket_row = dict(kind='flow-socket',source='agt1',flow=flow,
                digest=hashlib.sha256('\x00'.join(identity['tuple']).encode()).digest())
            values = [0]*16
            values[3],values[12],values[14],values[15] = 3,8<<20,1,1
            records.events.extend([socket_row,dict(kind='flow-state',source='agt1',flow=flow,values=values)])
        flow = bytes([200])*16
        records.events.extend([dict(kind='flow-socket',source='brk3',flow=flow,
            digest=hashlib.sha256('\x00'.join(owner.capacity_identity['tuple']).encode()).digest()),
            dict(kind='flow-capacity',source='brk3',flow=flow,values=[10,256<<20,256<<20,1,77]+[0]*11)])
        domains = dict(exec=dict(reservation=0),pty=dict(reservation=0),flow=dict(reservation=256<<20))
        request = dict(target=copy.deepcopy(target), kernel={}, profile='capacity-32',broker_kernels={'brk3':{}})
        original = copy.deepcopy(records.events)
        with mock.patch.object(observation,'fetch'), mock.patch.object(records,'ready'),mock.patch.object(records,'domains',return_value=domains):
            for label in ('capacity-full','capacity-refused'):
                self.assertEqual(observation.product_receipt(owner,label,request)['active_flows'],32)
            for defect in ('count','binding','dead','reservation','reason','pending','native-budget','no-refusal'):
                records.events = copy.deepcopy(original)
                if defect=='count': request['target']['flows'].pop('saturation-31')
                if defect=='binding': records.events[0]['digest']=bytes(32)
                if defect=='dead': records.events[1]['values'][14]=0
                if defect=='reservation': records.events[1]['values'][12]=0
                if defect=='reason': records.events[-1]['values'][0]=14
                if defect=='pending': records.events[-2]['digest']=bytes(32)
                if defect=='native-budget': records.events[-1]['values'][1]=224<<20
                if defect=='no-refusal': records.events.pop()
                with self.subTest(defect=defect),self.assertRaises(observation.CustodyError):
                    observation.product_receipt(owner,'capacity-refused',request)
                request['target']=copy.deepcopy(target)
            records.events=copy.deepcopy(original)
            for event in records.events:
                if event['kind']=='flow-state':
                    event['values'][3],event['values'][12],event['values'][14],event['values'][15]=5,0,0,0
            domains['flow']['reservation']=0
            self.assertEqual(observation.product_receipt(owner,'capacity-released',request)['settled_originals'],32)
            records.events[1]['values'][12]=8<<20
            with self.assertRaises(observation.CustodyError): observation.product_receipt(owner,'capacity-released',request)

    def test_saturation_fin_keeps_complete_tail_on_original_tcp(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            owners = {role:peer.Peer(root/role,NONCE,role) for role in ('client','target')}
            listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1)
            client=socket.create_connection(listener.getsockname());target,_=listener.accept()
            self.addCleanup(listener.close);self.addCleanup(client.close);self.addCleanup(target.close)
            spec=dict(id='saturation-0',protocol='raw',**{'class':'normal'},size=16,finite_fin=True,
                      up=peer.declaration('saturation-0.up',NONCE,21,16),down=peer.declaration('saturation-0.down',NONCE,21,16))
            for role,stream in (('client',client),('target',target)):
                self.addCleanup(owners[role].server.close)
                flow=peer.Flow(owners[role],'saturation-0',stream,spec)
                owners[role].flows['saturation-0']=flow
                flow.drain.set()
                flow.run()
            try:
                with fixture.ThreadPoolExecutor(max_workers=2) as workers:
                    final={role:workers.submit(owner.command,dict(command='drain-saturation',names=['saturation-0'])) for role,owner in owners.items()}
                    final={role:task.result() for role,task in final.items()}
                for role,row in final.items():
                    flow=row['flows']['saturation-0']
                    self.assertEqual([e['kind'] for e in flow['events']],['eof'])
                    self.assertEqual(len(flow['sent']),21)
                    self.assertEqual(len(flow['received']),21)
                    self.assertGreaterEqual(flow['fin_at'],flow['sent'][-1]['at'])
                    fixture.probe.check_identity(flow['identities'],True)
                self.assertEqual((root/'client/saturation-0.sent').read_bytes(),(root/'target/saturation-0.received').read_bytes())
                self.assertEqual((root/'target/saturation-0.sent').read_bytes(),(root/'client/saturation-0.received').read_bytes())
                # Retired TCP tuple loss never authorizes an FD replacement.
                flow=owners['client'].flows['saturation-0']
                replacement=socket.socket()
                try:
                    os.dup2(replacement.fileno(),flow.stream.fileno())
                    flow.snapshot_identity()
                    self.assertTrue(flow.identities[-1]['owner_lost'])
                finally: replacement.close()
                for owner in owners.values(): self.assertEqual(owner.command(dict(command='retire-saturation',names=['saturation-0']))['retired'],1)
            finally:
                for owner in owners.values(): owner.stop.set()
                client.close();target.close()
                for owner in owners.values():
                    for flow in owner.flows.values():
                        for task in flow.threads: task.join(timeout=2)

    def test_capacity_exec_counts_only_the_exact_native_error_and_recovery(self):
        owner = peer.Peer(self.root/'peer',NONCE,'client')
        self.addCleanup(owner.server.close)
        for rc,stdout,stderr,expected in ((70,b'',b'error: exec: workload: replay capacity exhausted\n',True),
                (0,b'',b'error: exec: workload: replay capacity exhausted\n',False),
                (70,b'',b'error: exec: broken pipe\n',False),(124,b'',b'error: exec: workload: replay capacity exhausted\n',False),
                (70,b'partial effect',b'error: exec: workload: replay capacity exhausted\n',False)):
            with mock.patch.object(peer,'run_supervised',return_value=SimpleNamespace(returncode=rc,stdout=stdout,stderr=stderr)):
                result = owner.command(dict(command='capacity-exec'))
                self.assertEqual(result['error_type']=='workload.ErrBackpressure',expected)
                self.assertEqual(result['native_rc'],rc)
        with mock.patch.object(peer,'run_supervised',return_value=SimpleNamespace(returncode=0,stdout=(NONCE+'-capacity\n').encode(),stderr=b'')):
            self.assertTrue(owner.command(dict(command='capacity-exec'))['success'])


class CollectorTests(unittest.TestCase):
    def setUp(self):
        # origin: hot-update S10 plan §6 / P11-P12 seam. Collector deadlines
        # must not replace the shared launcher's real READY/Join clock.
        clock = mock.patch.object(fixture, 'time', SimpleNamespace(monotonic=time.monotonic, sleep=time.sleep))
        clock.start()
        self.addCleanup(clock.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="s5-fixture-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.control = dict(kernel=kernel(), snapshot=dict(identity=dict(pid=50,boot_id="boot",start_ticks=50),flows={}),
                            sql=[dict(sid="lab",nid="agt1",name="flow-data",port=14001,home_broker="brk1",epoch=0,state="ALLOCATED")])
        self.write_control()
        script = self.root / "transport.py"
        script.write_text(SHIM)
        args = SimpleNamespace(root=self.root, nonce=NONCE, profile="normal", here=ROOT, instance="isolated-test",
                               sim="/fake/simcluster", arm="A")
        self.transport = fixture.Transport(args)
        self.transport.prefix = [sys.executable, "-I", "-B", str(script), str(self.root)]
        self.fixture = fixture.Fixture(args, self.transport)
        self.addCleanup(self.transport.close)
        # Even assertion failures release the fake per-node keepers naturally.
        self.addCleanup(lambda: [(self.root/('stop-'+node)).touch() for node in ('ctl1', 'agt1')])
        self.fixture.logs_before = self.fixture.log_snapshot("before")
        self.fixture.snapshots["before"] = dict(agent=kernel(), client=self.control["snapshot"], target=self.control["snapshot"])
        self.addCleanup(lambda: self.fixture.observer.join(timeout=3) if self.fixture.observer is not None else None)
        self.addCleanup(self.fixture.shutdown.set)

    def write_control(self):
        peer.publish(self.root / "control.json", self.control)

    def error(self, criterion, action, *args):
        with self.assertRaisesRegex(fixture.SetupError, criterion):
            action(*args)

    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.A
    def test_opening_carrier_baseline_requires_both_original_duplex_traces(self):
        self.fixture.phase = "running"
        complete = dict(flows=dict(opening=dict(sent=[0]*20, received=[0]*20)))
        for node in ("ctl1", "agt1"):
            for direction in ("sent", "received"):
                calls = []
                def remote(owner, command):
                    calls.append((owner, command))
                    doc = copy.deepcopy(complete)
                    if owner == node:
                        doc["flows"]["opening"][direction].pop()
                    return doc
                with mock.patch.object(self.fixture, "remote_call", side_effect=remote), mock.patch.object(fixture.time, "monotonic", side_effect=[0, 6]):
                    self.error("opening_duplex_samples_missing", self.fixture.carrier_baseline)
                self.assertFalse(any(command == "mark-opening" for _, command in calls))
        with mock.patch.object(self.fixture, "remote_call", return_value=complete) as remote:
            self.assertTrue(self.fixture.command(["carrier-baseline"])["marked"])
            self.assertEqual(remote.call_args_list, [mock.call("ctl1", "snapshot"), mock.call("agt1", "snapshot"),
                                                     mock.call("ctl1", "mark-opening"), mock.call("agt1", "mark-opening")])
        source = (LIB/"hot_update_flow_faults.sh").read_text().split("flow_carrier_rebuild() {", 1)[1].split("\n}", 1)[0]
        self.assertLess(source.index("flow_fixture carrier-baseline || return 2"), source.index("ss -K"))

    def test_native_boundary_status_is_preserved(self):
        policy=(LIB/'flow_fixture_transport.sh').read_text().split('    policy)',1)[1].split('    observe-start)',1)[0]
        self.assertLess(policy.index('FLOW_POLICY_PHASE=provision'),policy.index('agent_provision_yaml agt1'))
        self.assertLess(policy.index('agent_provision_yaml agt1'),policy.index('FLOW_POLICY_PHASE=allowlist'))
        self.assertLess(policy.index('FLOW_POLICY_PHASE=allowlist'),policy.index('upgradecfg_allow_agent agt1'))
        self.control["native_rc"] = 19
        self.write_control()
        self.error("transport_native_rc_19", self.transport.run, "ctl", "proxy", "on", "--yes")
        self.error("transport_native_rc_19_ctl_proxy", self.transport.run, "ctl", "proxy", "sub", "create", "--name", "private-name")
        # origin: hot-update S5 plan §9.4 / DRILL-RUN. Preserve only the
        # reviewed public rejection code, never the CLI's private detail.
        self.control.update(native_rc=75, native_error="error: expose failed: private-material (agent_rejected:home_catching_up)")
        self.write_control()
        with self.assertRaises(fixture.SetupError) as failed:
            self.transport.run("ctl", "expose", "agt1")
        self.assertEqual(str(failed.exception), "transport_native_rc_75_ctl_expose_agent_rejected_home_catching_up")
        self.control['native_error']="error: expose failed: private-material (private_material)"
        self.write_control()
        with self.assertRaises(fixture.SetupError) as failed:
            self.transport.run("ctl", "expose", "agt1")
        self.assertEqual(str(failed.exception), "transport_native_rc_75_ctl_expose")
        for phase in ('provision','allowlist','private_material'):
            self.control.update(native_rc=1,native_error='private-material\nFLOW_POLICY_PHASE='+phase+'\n')
            self.write_control()
            with self.assertRaises(fixture.SetupError) as failed:
                self.transport.run('policy')
            self.assertEqual(str(failed.exception),'transport_native_rc_1_policy'+('_'+phase if phase!='private_material' else ''))

        for result in ('start-limit-hit','exit-code','success','private_material'):
            self.control.update(native_rc=1,native_error='private-material\nFLOW_POLICY_PHASE=provision\nFLOW_POLICY_RESULT='+result+'\n')
            self.write_control()
            with self.assertRaises(fixture.SetupError) as failed:self.transport.run('policy')
            self.assertEqual(str(failed.exception),'transport_native_rc_1_policy_provision'+('_'+result.replace('-','_') if result!='private_material' else ''))


    # origin: hot-update S5 plan §9.4 / DRILL-RUN. Classification must
    # retain the native failed oracle, without reflecting private error text.
    def test_failed_product_receipt_keeps_public_criterion(self):
        with mock.patch.object(self.transport, "json", return_value=dict(rc=2, criterion="opening_fd_changed")) as call:
            self.error("product_receipt_opening_fd_changed", self.fixture.remote_call, "ctl1", "product-receipt")
            self.assertEqual(call.call_args.kwargs["allowed"], (0,2))
        with mock.patch.object(self.transport, "json", return_value=dict(rc=2, criterion="private key: value")):
            self.error("remote_observation_failed", self.fixture.remote_call, "ctl1", "product-receipt")

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Four independent
    # censuses must not spend four serial round trips in a two-second OPEN.
    def test_checkpoint_censuses_share_one_observation_window(self):
        barrier = threading.Barrier(4)
        def observe(value):
            try:
                barrier.wait(timeout=2)
            except threading.BrokenBarrierError:
                self.fail("checkpoint serialized independent censuses")
            return value
        with mock.patch.object(self.fixture, "kernel", side_effect=lambda: observe(kernel())), \
             mock.patch.object(self.fixture, "remote_call", side_effect=lambda *args, **fields: observe(self.control["snapshot"])), \
             mock.patch.object(self.fixture, "sql", side_effect=lambda *args: observe(self.control["sql"])):
            self.fixture.checkpoint("before")
        saved = fixture.probe.read_json(self.root / "checkpoint-before.json")
        self.assertEqual(saved["agent"], kernel())
        self.assertEqual(saved["sql"], self.control["sql"])

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Slow unrelated log
    # streams must not serialize detection of an actual agent cut barrier.
    def test_four_log_cursors_share_one_observation_window(self):
        barrier = threading.Barrier(4)
        original = self.transport.run
        def run(*args, **fields):
            try:
                barrier.wait(timeout=2)
            except threading.BrokenBarrierError:
                self.fail("four log streams were read serially")
            return original(*args, **fields)
        with mock.patch.object(self.transport, "run", side_effect=run):
            observed = self.fixture.log_snapshot("concurrent")
        self.assertEqual([row[0] for row in observed],
                         ["broker_slog", "broker_panic", "agent_slog", "agent_panic"])

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. proxy on publishes a
    # directive; its reply does not prove that the SS listener is ready.
    def test_subscription_supply_waits_for_actual_proxy_ready(self):
        states = [dict(enabled=True,nodes=[]),
                  dict(enabled=True,nodes=[dict(nid="other",ready=True)]),
                  dict(enabled=True,nodes=[dict(nid="agt1",ready=False)]),
                  dict(enabled=True,subscribers=[dict(token='private-material')],nodes=[dict(nid="agt1",ready=True)])]
        with mock.patch.object(self.transport, "json", side_effect=states) as status, mock.patch.object(fixture.time, "sleep"):
            self.fixture.wait_proxy_ready()
            self.assertEqual(status.call_count, 4)
            status.assert_called_with("ctl", "proxy", "status", "--json")
        receipt = json.loads((self.root/'proxy-readiness.json').read_text())
        self.assertEqual(receipt['nodes'],[dict(nid='agt1',ready=True)])
        self.assertNotIn('private-material',json.dumps(receipt))
        with mock.patch.object(self.transport, "json", return_value=dict(enabled=False,nodes=[dict(nid="agt1",ready=True)])), \
             mock.patch.object(fixture.time, "monotonic", side_effect=[0,16]):
            self.error("proxy_listener_not_ready", self.fixture.wait_proxy_ready)

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. The cluster reaper
    # delivers later keysets asynchronously; originals must start authorized.
    def test_subscription_empty_replica_shares_original_readiness_deadline(self):
        empty = dict(rc=2, criterion='subscription_node_not_ready')
        ready = dict(host='brk1', port=14000)
        with mock.patch.object(self.transport, 'run', return_value=b'private'), mock.patch.object(self.transport, 'json', side_effect=[empty,ready]) as install, mock.patch.object(fixture.time, 'sleep'):
            self.assertEqual(self.fixture.subscription_key('bob','private','127.0.0.1',9000,time.monotonic()+1),ready)
            self.assertEqual(install.call_count,2)
        receipt = json.loads((self.root/'subscription-readiness.json').read_text())
        self.assertEqual((receipt['name'],receipt['attempts'],receipt['ready']),('bob',2,True))
        self.assertLess(receipt['at'],receipt['deadline'])
        self.assertNotIn('private',json.dumps(receipt))
        with mock.patch.object(self.transport, 'run', return_value=b'private'), mock.patch.object(self.transport, 'json', return_value=empty):
            self.error('subscription_replica_not_ready', self.fixture.subscription_key,'bob','private','127.0.0.1',9000,0)
        with mock.patch.object(self.transport, 'run', return_value=b'private'), mock.patch.object(self.transport, 'json', return_value=dict(rc=2,criterion='peer_observation_failed')):
            self.error('subscription_supply_invalid',self.fixture.subscription_key,'bob','private','127.0.0.1',9000,time.monotonic()+1)

    def test_initial_listener_is_enabled_after_complete_subscription_creation(self):
        self.fixture.prepare()
        self.assertEqual(json.loads((self.root/'cipher-pressure.json').read_text()),dict(loopback_mtu=1500))
        calls = [json.loads(line) for line in (self.root / "calls").read_text().splitlines()]
        creates = [index for index, row in enumerate(calls) if row[:4] == ["ctl", "proxy", "sub", "create"]]
        enable = calls.index(["ctl", "proxy", "on", "--yes"])
        ready = calls.index(["ctl", "proxy", "status", "--json"])
        supplies = [index for index, row in enumerate(calls) if row[:3] == ["input", "brk1", "curl"]]
        self.assertEqual(len(creates), 2)
        self.assertEqual(len(supplies), 2)
        self.assertLess(max(creates), enable)
        self.assertLess(enable, ready)
        self.assertLess(ready, min(supplies))

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. An exposed local
    # port dials loopback, even when the independent SS target uses the NIC.
    def test_open_syn_fault_targets_actual_expose_loopback_address(self):
        self.control["agent_ip"] = "172.18.0.3"
        self.write_control()
        self.fixture.prepare()
        document = json.loads((self.root/"fixture.json").read_text())
        self.assertEqual(document["open"], dict(ip="127.0.0.1", port=9002))

    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
    def test_original_business_requires_actual_short_write_network_fault(self):
        original = self.transport.run
        def run(*args,**fields):
            if args[0]=='observe-start': return b'{"cipher_pressure_loopback_mtu":65536}\n'
            return original(*args,**fields)
        with mock.patch.object(self.transport,'run',side_effect=run):
            self.error('cipher_short_write_network_fault_unavailable',self.fixture.prepare)
        self.assertEqual(self.fixture.phase,'new')
        self.assertFalse((self.root/'cipher-pressure.json').exists())

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Evidence paths exceed
    # sockaddr_un on the real handoff host; startup must retain that evidence.
    def test_collector_socket_accepts_long_evidence_directory(self):
        root = self.root / ("evidence-" + "x" * 90) / "arm-A"
        root.mkdir(parents=True)
        self.assertGreater(len(os.fsencode(root / "fixture.sock")), 108)
        args = copy.copy(self.fixture.args)
        args.root = root
        shutdown = threading.Event()
        errors = []
        def command(argv):
            if argv == ["cleanup"]:
                shutdown.set()
            return dict(command=argv)
        control = SimpleNamespace(shutdown=shutdown, command=command, cleanup=lambda: None)
        def exercise():
            try:
                fixture.serve(args)
            except BaseException as exc:
                errors.append(exc)
        with mock.patch.object(fixture, "Fixture", return_value=control):
            thread = threading.Thread(target=exercise, daemon=True)
            thread.start()
            try:
                deadline = time.monotonic() + 2
                while not (root / "fixture.sock").exists() and thread.is_alive() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertEqual(errors, [])
                self.assertEqual(fixture.call(root, ["snapshot"]), dict(command=["snapshot"]))
                self.assertEqual(fixture.call(root, ["cleanup"]), dict(command=["cleanup"]))
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])
                self.assertFalse((root / "fixture.sock").exists())
            finally:
                shutdown.set()

    def test_all_profiles_declare_finite_original_cohorts_before_start(self):
        for profile in fixture.PROFILES:
            with self.subTest(profile=profile):
                args = copy.copy(self.fixture.args)
                args.profile=profile; args.root=self.root/profile
                collector=fixture.Fixture(args,self.transport)
                collector.prepare()
                declarations=json.loads((args.root/'declarations.json').read_text())['flows']
                self.assertEqual(len(declarations),11 if profile=='silent-revoke' else 9)
                self.assertTrue(all(row['up']['records']>0 and row['down']['records']>0 for row in declarations))
                names = {row['id']: row for row in declarations}
                if profile in ('legacy-broker', 'legacy-exporter'):
                    self.assertNotIn('half-close', names)
                    self.assertEqual(names['legacy-duplex']['class'], 'normal')
                    self.assertEqual(names['legacy-duplex']['up']['records'], 2400)
                    self.assertEqual(names['legacy-duplex']['down']['records'], 2400)
                else:
                    self.assertEqual(names['half-close']['class'], 'half-close')
                    self.assertGreater(names['half-close']['down']['bytes'], 8 << 20)

                self.assertFalse(any(key in row for row in declarations for key in ('master','password','target_host','target_port')))
                self.assertFalse(any('target_host' in row or 'target_port' in row for row in collector.specs))
                self.assertEqual(json.loads((args.root/'fixture.json').read_text())['open'],dict(ip='127.0.0.1',port=9002))
                calls=[json.loads(line) for line in (self.root/'calls').read_text().splitlines()]
                self.assertFalse(any('/sub/' in item for row in calls for item in row))
                self.assertIn(['input','brk1','curl','-fsS','--max-time','6','--config','-'],calls)
                if profile == 'capacity-32':
                    capacity = json.loads((args.root/'capacity-declarations.json').read_text())
                    self.assertEqual(len(capacity['flows']),32)
                    self.assertEqual(capacity['reservation'],256<<20)
                    self.assertEqual(capacity['normal_limit'],224<<20)
                    self.assertTrue(all(row['finite_fin'] for row in capacity['flows']))
                    with self.assertRaisesRegex(fixture.SetupError,'normal_before_capacity_recovery'):
                        collector.start()
                    collector.capacity_complete = True  # Only this process-boundary control bypasses live saturation.
                collector.start()
                self.assertEqual(collector.phase,'running')
                collector.cleanup()

    def test_baseline_requires_twenty_independent_samples_in_every_direction(self):
        self.fixture.phase='running'
        self.fixture.specs=[dict(id='tcp-0')]
        value=dict(sent=[{}]*20,received=[{}]*20)
        self.control['snapshot']['flows']={'tcp-0':value}
        self.write_control()
        with mock.patch.object(self.fixture,'start_observer'):
            self.fixture.baseline()
        value['received']=[]; self.write_control()
        with mock.patch.object(fixture.time,'monotonic',side_effect=[0,21]):
            self.error('baseline_duplex_samples_missing',self.fixture.baseline)

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. A successful duplex
    # sample count cannot replace the required actual cipher-write prior.
    def test_baseline_waits_for_original_client_and_product_partial_writes(self):
        self.fixture.phase = "running"
        self.fixture.specs = [dict(id="bob", protocol="ss")]
        self.control["snapshot"]["flows"] = {"bob": dict(sent=[{}]*20, received=[{}]*20)}
        self.write_control()
        with mock.patch.object(self.fixture, "product_receipt", return_value=dict(custody_complete=True, partial_ready=False)), \
             mock.patch.object(fixture.time, "monotonic", side_effect=[0, 21]):
            self.error("baseline_duplex_samples_missing", self.fixture.baseline)
        with mock.patch.object(self.fixture, "product_receipt", return_value=dict(custody_complete=True, partial_ready=True)) as receipt, \
             mock.patch.object(self.fixture, "start_observer"):
            self.fixture.baseline()
            self.assertEqual(receipt.call_args_list[0].args[0], "partial-ready")

    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN. Parent umask 077
    # must retain private capture modes without denying the remote keeper.
    def test_remote_judge_owns_copied_inputs_under_private_umask(self):
        self.fixture.phase = "collected"
        self.fixture.specs = []
        self.fixture.snapshots["final"] = dict(client=dict(flows={}), target=dict(flows={}))
        copied = {}
        original_umask = os.umask(0o077)
        try:
            for role in ("client", "target"):
                folder = self.root/role; folder.mkdir()
                (folder/"capture").write_bytes(b"public application frame")
            def transport(command, *args):
                if command == "copy-to":
                    source, _, destination = args
                    copied[destination] = dict(owner="root", mode=source.stat().st_mode & 0o777)
                elif command == "exec" and args[:4] == ("ctl1", "chown", "-R", "sim:sim"):
                    for destination in args[4:]:
                        copied[destination]["owner"] = "sim"
                else:
                    self.fail("unexpected transfer operation")
            def judge(node, command, **fields):
                self.assertEqual((node, command), ("ctl1", "judge"))
                self.assertEqual(set(copied), {self.fixture.remote+"/"+name for name in ("evidence.json", "client", "target")})
                self.assertTrue(all(row["owner"] == "sim" for row in copied.values()), "private copied input remains unreadable by the keeper")
                self.assertEqual([copied[self.fixture.remote+"/"+name]["mode"] for name in ("evidence.json", "client", "target")], [0o600, 0o700, 0o700])
                return dict(rc=1, verdict="ASSERT-FAIL", criterion="no_reset")
            with mock.patch.object(self.fixture, "request_evidence", return_value=({}, True)), \
                 mock.patch.object(self.fixture, "read_events", return_value=[dict(msg=fixture.EVENTS["resume"], carrier_generation=1)]), \
                 mock.patch.object(self.transport, "run", side_effect=transport), \
                 mock.patch.object(self.fixture, "remote_call", side_effect=judge):
                self.assertEqual(self.fixture.continuity()["rc"], 1)
        finally:
            os.umask(original_umask)

    def test_remote_judge_native_failure_is_preserved(self):
        self.control["judge"] = dict(rc=1, verdict="ASSERT-FAIL", criterion="no_reset")
        self.write_control()
        self.assertEqual(self.fixture.remote_call("ctl1", "judge")["rc"], 1)

    def test_remote_failed_snapshot_cannot_become_success(self):
        self.control["snapshot"] = dict(rc=2)
        self.write_control()
        self.error("remote_observation_failed", self.fixture.remote_call, "ctl1", "snapshot")

    def test_barrier_is_exact_and_consumed_once(self):
        self.control["agent_slog"] = ('level=INFO msg="diagnostic tunnel: flow handoff prepared" carrier_generation=2\n'
            'level=INFO msg="tunnel: flow handoff prepared" carrier_generation=3 generation=2\n')
        self.write_control()
        result = self.fixture.wait_event("prepare", .2)
        self.assertEqual(result["event_seq"], 2)
        self.error("barrier_not_observed", self.fixture.wait_event, "prepare", .05)

    def test_barrier_requires_generation_fields(self):
        self.control["agent_slog"] = 'level=INFO msg="tunnel: flow carrier resumed"\n'
        self.write_control()
        self.error("barrier_fields_missing", self.fixture.wait_event, "resume", .05)

    def test_final_cut_marks_both_independent_observers(self):
        self.control["agent_slog"] = 'level=INFO msg="agent: image final cut" generation=3\n'
        self.write_control()
        self.fixture.wait_event("final-cut", .2)
        calls = [json.loads(line) for line in (self.root / "calls").read_text().splitlines()]
        inputs = [argv for argv in calls if argv[0] == "input"]
        self.assertEqual([argv[1] for argv in inputs], ["ctl1", "agt1"])

    def test_background_cut_observer_marks_real_barrier_and_records_read_failure(self):
        self.control['agent_slog']='level=INFO msg="agent: image final cut" generation=3\n'
        self.write_control()
        self.fixture.start_observer()
        try:
            deadline=time.monotonic()+3
            while not (self.root/'cut-1.json').exists():
                self.assertLess(time.monotonic(),deadline); time.sleep(.01)
            value=json.loads((self.root/'cut-1.json').read_text())
            self.assertEqual(value['generation'],3)
            self.assertEqual(set(value['observers']),{'ctl1','agt1'})
            self.control['native_rc']=19; self.write_control()
            self.fixture.observer.join(timeout=3)
            self.assertFalse(self.fixture.observer.is_alive())
            self.assertEqual(self.fixture.observer_errors,['cut_observation_failed'])
        finally:
            self.fixture.shutdown.set(); self.fixture.observer.join(timeout=3)

    def test_carrier_rebuild_cannot_reuse_old_resume(self):
        self.control["agent_slog"] = 'level=INFO msg="tunnel: flow carrier resumed" carrier_generation=1\n'
        self.write_control()
        self.fixture.event_baseline = 1
        self.error("barrier_not_observed", self.fixture.wait_event, "carrier-restored", .05)
        self.control["agent_slog"] += 'level=INFO msg="tunnel: flow carrier resumed" carrier_generation=2\n'
        self.write_control()
        self.assertEqual(self.fixture.wait_event("carrier-restored", .2)["record"]["carrier_generation"], 2)

    def test_revoke_resume_cannot_reuse_initial_business_binding(self):
        self.control['agent_slog']='level=INFO msg="tunnel: flow carrier resumed" carrier_generation=1 owner_generation=1 mode=1\n'
        self.write_control(); self.fixture.event_baseline=1
        self.error('barrier_not_observed',self.fixture.wait_event,'resume',.05)
        self.control['agent_slog']+='level=INFO msg="tunnel: flow carrier resumed" carrier_generation=2 owner_generation=1 mode=2\n'
        self.write_control()
        self.error('barrier_not_observed',self.fixture.wait_event,'resume',.05)
        self.control['agent_slog']+='level=INFO msg="tunnel: flow carrier resumed" carrier_generation=3 owner_generation=2 mode=4\n'
        self.write_control()
        self.assertEqual(self.fixture.wait_event('resume',.2)['record']['owner_generation'],2)
        # origin: hot-update S5 plan §9.1 / T48. A prior candidate's valid
        # activation still cannot satisfy the next fault window's barrier.
        self.fixture.consumed.clear()
        self.fixture.event_baseline=3
        self.error('barrier_not_observed',self.fixture.wait_event,'resume',.05)
        self.control['agent_slog']+='level=INFO msg="tunnel: flow carrier resumed" carrier_generation=4 owner_generation=3 mode=4\n'
        self.write_control()
        self.assertEqual(self.fixture.wait_event('resume',.2)['record']['owner_generation'],3)

    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN. The restored
    # client's native default logger writes its accepted activation to stderr.
    def test_restored_carrier_stderr_barrier_keeps_stable_event_identity(self):
        self.control['agent_slog']='level=INFO msg="tunnel: flow handoff prepared" carrier_generation=1 generation=2\n'
        self.write_control()
        self.fixture.event_baseline=len(self.fixture.read_events())
        self.assertEqual(self.fixture.event_baseline,1)
        self.control['agent_panic']='2026/10/04 13:13:54 INFO tunnel: flow carrier resumed owner_generation=2 carrier_generation=2 mode=4\n'
        self.write_control()
        records=self.fixture.read_events()
        self.assertEqual(len(records),2)
        self.assertEqual(records[1]['owner_generation'],2)
        activated=self.fixture.wait_event('resume',.2)
        self.assertEqual(activated['event_seq'],2)
        self.assertEqual(activated['record']['carrier_generation'],2)
        self.control['agent_slog']+='level=INFO msg="agent: image final cut" generation=3\n'
        self.write_control()
        self.error('barrier_not_observed',self.fixture.wait_event,'resume',.05)
        self.assertEqual(self.fixture.read_events()[activated['event_seq']-1],activated['record'])
        self.assertIsNone(fixture.record_message('diagnostic 2026/10/04 13:13:54 INFO tunnel: flow carrier resumed owner_generation=2 carrier_generation=2 mode=4'))

    def test_candidate_boot_requires_running_image_and_persisted_count(self):
        marker = dict(state="pending",boot_count=1,new_sha="a"*64)
        self.control["kernel"]["marker"] = marker
        self.write_control()
        self.assertEqual(self.fixture.wait_event("candidate-boot-persisted", .1)["marker"], marker)
        for field, value in (("boot_count",0),("state","committed"),("new_sha","b"*64)):
            previous = marker[field]; marker[field] = value; self.write_control()
            self.error("barrier_not_observed", self.fixture.wait_event, "candidate-boot-persisted", .05)
            marker[field] = previous

    def test_watchdog_redispatch_does_not_prove_completed_rollback(self):
        self.control["agent_slog"] = 'level=INFO msg="agent: image re-dispatched" generation=3\n'
        self.control["kernel"]["marker"] = dict(state="pending")
        peer.publish(self.root/'pending-marker.json', dict(upgrade_id='original-upgrade',new_sha='candidate',generation=2))
        self.write_control()
        with mock.patch.object(self.fixture, 'remote_call', return_value=dict(observation=None)):
            self.error("barrier_not_observed", self.fixture.wait_event, "watchdog-rollback", .05)
        pending = dict(upgrade_id='original-upgrade', new_sha='candidate', generation=2)
        peer.publish(self.root/'pending-marker.json', pending)
        terminal = dict(pending, state='rolled_back', prev_sha=self.control['kernel']['running_sha'])
        terminal['generation'] = 3
        captured = dict(at=time.monotonic(), marker=terminal)
        self.control['kernel']['marker'] = None
        self.write_control()
        with mock.patch.object(self.fixture, 'remote_call', return_value=dict(observation=captured)):
            self.assertEqual(self.fixture.wait_event("watchdog-rollback", .2)["event"], "watchdog-rollback")
            for field, value in (('state','pending'), ('upgrade_id','different'), ('new_sha','other'), ('generation',2)):
                old = terminal[field]; terminal[field] = value
                self.fixture.consumed.clear()
                self.error("barrier_not_observed", self.fixture.wait_event, "watchdog-rollback", .05)
                terminal[field] = old

    def test_checkpoint_rejects_pid_reuse(self):
        self.control["kernel"]["identity"]["start_ticks"] += 1
        self.write_control()
        self.error("agent_identity_changed", self.fixture.checkpoint, "post-upgrade")

    def test_hop_requires_new_running_sha_and_committed_marker(self):
        self.control["kernel"]["marker"] = dict(state="committed", new_sha="a"*64)
        self.write_control()
        self.error("hop_running_sha_unchanged", self.fixture.checkpoint, "hop-1")
        self.control["kernel"]["running_sha"] = "b"*64
        self.control["kernel"]["marker"]["new_sha"] = "b"*64
        self.write_control()
        self.fixture.checkpoint("hop-1")
        self.control["kernel"]["marker"]["state"] = "pending"
        self.write_control()
        self.error("hop_not_committed", self.fixture.checkpoint, "hop-2")

    def test_refusal_retains_original_marker_and_image(self):
        self.fixture.checkpoint("refusal-unchanged")
        for field, value in (("running_sha","b"*64),("marker",dict(state="pending"))):
            original = self.control["kernel"][field]
            self.control["kernel"][field] = value; self.write_control()
            self.error("legacy_refusal_changed_image", self.fixture.checkpoint, "refusal-unchanged")
            self.control["kernel"][field] = original

    def test_empty_sql_is_missing_evidence(self):
        self.control["sql"] = []
        self.write_control()
        self.error("sql_allocation_receipt_empty", self.fixture.sql, "baseline")

    def test_original_pending_open_never_synthesizes_partial_cipher_write(self):
        self.control["kernel"]["connections"] = [dict(state="02",ino=40)]
        self.write_control()
        self.error("product_partial_cipher_observer_unavailable", self.fixture.checkpoint, "open-partial")
        document = json.loads((self.root / "open-partial.json").read_text())
        self.assertEqual(document["kernel_syn_sent"], [dict(state="02",ino=40)])
        self.assertNotIn("ss", document)

    def test_unavailable_private_priors_are_explicit_setup_failures(self):
        for label in ("revoked","revoke-races","ready-pending","active-progress","allocation","legacy-transport"):
            with self.subTest(label=label):
                self.error("required_product_observation_unavailable", self.fixture.checkpoint, label)
                self.assertTrue(json.loads((self.root / (label+"-missing.json")).read_text())["missing_observations"])

    # origin: hot-update S5 plan §9.4 / DRILL-RUN. Keep the actual failed
    # oracle criterion in the native receipt after preserving missing inputs.
    def test_checkpoint_retains_failed_product_criterion(self):
        with mock.patch.object(self.fixture,'product_receipt',side_effect=fixture.SetupError('product_receipt_victim_not_closed')):
            self.error('product_receipt_victim_not_closed',self.fixture.checkpoint,'revoke-races')
        self.assertTrue(json.loads((self.root/'revoke-races-missing.json').read_text())['missing_observations'])

    def test_watchdog_receipt_reports_known_fields_without_minting_missing_priors(self):
        self.control['kernel']['marker']=dict(state='rolled_back',generation=3)
        self.write_control()
        peer.publish(self.root/'pending-marker.json',dict(state='pending',generation=2))
        self.error('required_product_observation_unavailable',self.fixture.checkpoint,'watchdog')
        value=json.loads((self.root/'watchdog.json').read_text())
        self.assertEqual(value['generation_after'],3)
        self.assertTrue(value['missing_observations'])
        self.assertNotIn('fresh_ss_grant',value)

    def test_legacy_exec_uses_durable_successor_commit_without_requiring_old_image_log(self):
        self.control['kernel']['running_sha']='b'*64
        self.control['kernel']['marker']=dict(state='committed',new_sha='b'*64,boot_count=1)
        self.control['agent_slog']='level=INFO msg="agent: upgrade COMMITTED (registered as the new binary)"\n'
        self.write_control()
        self.fixture.checkpoint('legacy-fault')
        value=json.loads((self.root/'legacy-fault.json').read_text())
        self.assertEqual(value['event_seq'],1)
        self.assertEqual(value['before']['pid'],value['after']['pid'])
        self.assertNotEqual(value['before']['running_sha'],value['after']['running_sha'])
        # This negative is an independent log, rather than a live prefix
        # rewrite after accepting a commit. Keep the real prefix guard active.
        # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
        previous = self.fixture
        self.fixture = fixture.Fixture(previous.args, self.transport)
        self.fixture.logs_before = previous.logs_before
        self.fixture.snapshots['before'] = previous.snapshots['before']
        self.control['agent_slog']='level=INFO msg="diagnostic COMMITTED"\n'; self.write_control()
        self.error('legacy_exec_event_missing',self.fixture.checkpoint,'legacy-fault')

    def test_cleanup_saves_live_observations_before_closing_original_owners(self):
        self.fixture.phase='running'
        self.fixture.cleanup()
        self.assertTrue((self.root/'cleanup-agent.json').is_file())
        self.assertEqual(json.loads((self.root/'cleanup-agent.json').read_text())['running_sha'],'a'*64)
        self.assertEqual((self.root/'commands').read_text().splitlines(),['snapshot','snapshot','cleanup','cleanup'])
        self.assertEqual(json.loads((self.root/'cleanup-capture.json').read_text())['status'],'INCOMPLETE')
        self.fixture.cleanup()
        self.assertEqual((self.root/'commands').read_text().splitlines(),['snapshot','snapshot','cleanup','cleanup'])

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. The remote collector
    # outlives a terminated docker exec client unless its schedule is ended.
    def test_cleanup_reaps_requests_before_transport_shutdown(self):
        self.fixture.phase = "running"
        self.fixture.request_job = mock.Mock()
        self.fixture.request_job.poll.return_value = None
        events = []
        self.fixture.request_job.wait.side_effect = lambda **kwargs: events.append("wait") or 0
        with mock.patch.object(self.fixture, "request_mark", side_effect=lambda name: events.append(name)), \
             mock.patch.object(self.transport, "close", side_effect=lambda: events.append("close")):
            self.fixture.cleanup()
        self.assertEqual(events, ["finish", "wait", "close"])
        self.fixture.request_job.wait.assert_called_once_with(timeout=20)
        self.assertEqual(json.loads((self.root/'cleanup-capture.json').read_text())['status'],'INCOMPLETE')
        self.fixture.cleanup()
        self.assertEqual((self.root/'commands').read_text().splitlines(),['snapshot','snapshot','cleanup','cleanup'])

    def test_finish_drains_both_endpoints_concurrently_and_reaps_requests(self):
        self.fixture.phase='running'
        self.fixture.request_job=mock.Mock()
        self.fixture.request_job.wait.return_value=0
        arrived=threading.Barrier(2)
        def boundary(node,command,**fields):
            self.assertEqual(command,'finish')
            arrived.wait(timeout=1)
            return {}
        with mock.patch.object(self.fixture,'remote_call',side_effect=boundary), \
                mock.patch.object(self.fixture,'checkpoint'):
            self.fixture.snapshots['final']=self.fixture.snapshots['before']
            self.fixture.finish()
        self.fixture.request_job.wait.assert_called_once_with(timeout=20)
        self.fixture.phase='running'
        self.fixture.request_job.wait.return_value=19
        with mock.patch.object(self.fixture,'remote_call',return_value={}):
            self.error('request_collector_native_failure',self.fixture.finish)

    def request_files(self):
        root=self.root/'requests'; root.mkdir(exist_ok=True); (root/'marks.d').mkdir(exist_ok=True)
        rows=[]
        mixed=fixture.load('hot_update_mixed_probe')
        for seq in range(60):
            _,row=mixed.request(seq,'agt1',NONCE)
            row.update(t_start=seq/10,t_end=seq/10+.2,latency_s=.2,rc=0,stderr_tail='')
            rows.append(row)
        metadata=dict(rate=10,duration_s=6,request_s=5,nonce=NONCE,complete=True,skipped=0,scheduled=60,finished_s=7)
        peer.publish(root/'run.json',metadata); peer.publish(root/'marks.json',dict(cohort=2,finish=4))
        (root/'samples.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows))
        (self.root/'request-ledger').write_text(''.join(row['seq_written']+'\n' for row in rows if row['class']=='write'))
        return rows,metadata

    def test_request_receipt_reuses_independent_judge_and_keeps_entire_schedule(self):
        rows,metadata=self.request_files()
        requests,accepted=self.fixture.request_evidence()
        self.assertTrue(accepted)
        self.assertEqual(len(requests['samples']),60)
        self.assertEqual([row['scheduled'] for row in requests['samples']],[seq/10 for seq in range(60)])
        receipt=json.loads((self.root/'requests.json').read_text())
        self.assertEqual(receipt['rc'],0)
        self.assertGreater(receipt['summary']['writes_in_window'],0)
        for field in ('duplicate','failure','over-budget','refusal'):
            with self.subTest(field=field):
                rows,metadata=self.request_files()
                if field=='duplicate':
                    with (self.root/'request-ledger').open('a') as out: out.write(rows[1]['seq_written']+'\n')
                else:
                    if field=='over-budget':
                        rows[0].update(t_end=6,latency_s=6)
                    else:
                        rows[21].update(rc=75 if field=='refusal' else 1,stderr_tail='agent_upgrading' if field=='refusal' else 'failure')
                    (self.root/'requests/samples.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows))
                _,accepted=self.fixture.request_evidence()
                self.assertFalse(accepted)
                self.assertEqual(json.loads((self.root/'requests.json').read_text())['rc'],1)

    def test_request_receipt_rejects_incomplete_evidence_and_another_cohort(self):
        rows,metadata=self.request_files()
        (self.root/'requests/samples.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows[:-1]))
        self.error('request_evidence_incomplete',self.fixture.request_evidence)
        self.request_files()
        metadata['nonce']='cd'*16; peer.publish(self.root/'requests/run.json',metadata)
        self.error('request_cohort_nonce_changed',self.fixture.request_evidence)

    def test_selftest_requires_both_independent_oracle_control_processes(self):
        for statuses in ((1,0),(0,1)):
            children=[SimpleNamespace(returncode=rc,stdout=b'',stderr=b'') for rc in statuses]
            with self.subTest(statuses=statuses), mock.patch.object(fixture.sys,'argv',['fixture','selftest']), \
                    mock.patch.object(peer,'run_supervised',side_effect=children),contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(fixture.main(),2)

    def test_command_order_and_labels_reject_foreign_input(self):
        for argv in (["start"],["baseline"],["finish"],["collect"],["continuity"],["open"],["prepare","normal"],["checkpoint","../private"],["wait-event","unknown","1"],["arm-revoke","resume"]):
            with self.subTest(argv=argv):
                with self.assertRaises(fixture.SetupError):
                    self.fixture.command(argv)

    def test_revoke_calls_control_plane_without_closing_victim(self):
        self.fixture.revoke("alice")
        calls = [json.loads(line) for line in (self.root / "calls").read_text().splitlines()]
        self.assertEqual(calls[-1], ["ctl","proxy","sub","revoke","alice"])
        self.assertFalse(any(argv[0] == "input" for argv in calls))

    # origin: hot-update S5 plan §9.4 / DRILL-RUN. Preserve the actual
    # rejected observation without publishing arbitrary exception contents.
    def test_collection_failure_retains_only_its_public_guard(self):
        self.fixture.phase = "finished"
        self.fixture.snapshots["final"] = dict(client=self.control["snapshot"], target=self.control["snapshot"])
        for failure, expected in (("product_receipt_domain_budget_exceeded", "product_receipt_domain_budget_exceeded"),
                                  ("private material: value", "collection_observation_failed")):
            with self.subTest(failure=failure), mock.patch.object(self.fixture, "product_receipt", side_effect=fixture.SetupError(failure)):
                result = self.fixture.collect()
                self.assertEqual(result["rc"], 2)
                self.assertEqual(json.loads((self.root/"collect-missing.json").read_text())["criterion"], expected)
                self.fixture.phase = "finished"

    def test_four_log_collection_cannot_bless_rotated_or_unreadable_stream(self):
        self.fixture.phase = "finished"
        self.control["agent_slog"] = "fresh record\n"
        self.write_control()
        # Copies after the four snapshots are process fakes; real log checking
        # still runs and must invalidate completion before any failure.
        self.fixture.snapshots["final"] = dict(client=self.control["snapshot"], target=self.control["snapshot"])
        result=self.fixture.collect()
        self.assertEqual(result['rc'],2)
        self.assertTrue(json.loads((self.root/'collect-missing.json').read_text())['missing_observations'])
        self.assertEqual(len(json.loads((self.root / "logs.json").read_text())["cursors"]), 4)
        self.fixture.phase = "finished"
        self.fixture.logs_before[0][1]["inode"] = 99
        self.error("log_rotated", self.fixture.collect)
        self.assertFalse((self.root / "logs.json").exists())


class LogCustodyTests(unittest.TestCase):
    def test_freeze_uses_existing_activation_message_and_keeps_calibration_overage(self):
        rows=[dict(msg='agent: image activated generation=2 tasks=1 freeze_window_ms='+str(ms)+' shim_ms=2 import_ms=3') for ms in (400,600)]
        self.assertEqual(fixture.freeze_values(rows),[.4,.6])
        self.assertEqual(fixture.freeze_values([dict(msg='diagnostic '+rows[0]['msg'])]),[])
        with self.assertRaisesRegex(fixture.SetupError,'freeze_calibration_fields_missing'):
            fixture.freeze_values([dict(msg='agent: image activated generation=2')])

    def test_log_identity_prefix_length_and_read_status_are_independent(self):
        before = dict(read_ok=True,inode=1,length=3)
        after = dict(read_ok=True,inode=1,length=6)
        self.assertEqual(fixture.log_suffix(before,b"old",after,b"oldnew"), b"new")
        for mutation in (dict(read_ok=False),dict(inode=2),dict(length=5)):
            with self.subTest(mutation=mutation), self.assertRaises(fixture.SetupError):
                fixture.log_suffix(before,b"old",dict(after,**mutation),b"oldnew")
        with self.assertRaises(fixture.SetupError):
            fixture.log_suffix(before,b"old",after,b"BADnew")

    def test_components_and_journal_cursors_cannot_rotate(self):
        part = dict(path="a",dev=1,inode=1,length=3)
        before = dict(read_ok=True,inode=1,length=3,parts=[part])
        after = dict(read_ok=True,inode=1,length=6,parts=[dict(part,length=6)])
        self.assertEqual(fixture.log_suffix(before,b"old",after,b"oldnew"), b"new")
        bad = copy.deepcopy(after); bad["parts"][0]["dev"] = 2
        with self.assertRaises(fixture.SetupError): fixture.log_suffix(before,b"old",bad,b"oldnew")
        before = dict(read_ok=True,inode=1,length=3,journal_identity="journal",journal_cursors=["one"])
        after = dict(read_ok=True,inode=1,length=6,journal_identity="journal",journal_cursors=["one","two"])
        self.assertEqual(fixture.log_suffix(before,b"old",after,b"oldnew"), b"new")
        for mutation in (dict(journal_identity="other"),dict(journal_cursors=["two"])):
            with self.subTest(mutation=mutation), self.assertRaises(fixture.SetupError):
                fixture.log_suffix(before,b"old",dict(after,**mutation),b"oldnew")

    def test_product_messages_pin_exact_barrier_positions_and_public_fields(self):
        # origin: hot-update S5 plan §9.1 / DRILL-RUN. A receipt after
        # SO_ERROR consumption cannot prove the original pending connector.
        pump = (REPO / "internal/flow/pump.go").read_text()
        connector = pump[pump.index("func runPumps"):pump.index("func stopped")]
        self.assertIn("observeRecordLocked(r)", connector)
        self.assertLess(connector.index("r.raw = raw"), connector.index("observeRecordLocked(r)"))
        self.assertLess(connector.index("observeRecordLocked(r)"), connector.index("raw.CompleteConnect("))
        self.assertIn("if drillobserve.Hook != nil {\n\t\t\tobserveRecordLocked(r)", connector)
        tunnel = (REPO / "internal/tunnel/flow_client.go").read_text()
        agent = (REPO / "internal/agent/upgrade_image.go").read_text()
        # origin: hot-update S5 plan §9.1 / GATES-FINAL-3. Follow the
        # extracted binding helper as well as its checked call before logging;
        # source order across separate functions is not execution order.
        resume = tunnel[tunnel.index("func attachClientFlow"):tunnel.index("func beginClientFlowResume")]
        binding = tunnel[tunnel.index("func bindClientFlowResume"):tunnel.index("func (c *Client) PrepareHandoff")]
        prepare = tunnel[tunnel.index("func (c *Client) PrepareHandoff"):tunnel.index("func prepareClientEndpoint")]
        recovery = tunnel[tunnel.index("func prepareClientEndpoint"):tunnel.index("func channelErrors")]
        final = agent[agent.index("func prepareImageCut"):]
        self.assertEqual(tunnel.count('Info("tunnel: flow handoff prepared"'),1)
        self.assertEqual(tunnel.count('Info("tunnel: flow carrier resumed"'),1)
        self.assertEqual(agent.count('Info("agent: image final cut"'),1)
        self.assertIn("if err := bindClientFlowResume(st, e, resp, a); err != nil {\n\t\treturn err\n\t}", resume)
        self.assertIn("flow.Bind(", binding)
        self.assertLess(binding.index("flow.Bind("), binding.index("return nil"))
        self.assertLess(resume.index("bindClientFlowResume("), resume.index('Info("tunnel: flow carrier resumed"'))
        self.assertLess(resume.index("abortClientPermit("), resume.index('Info("tunnel: flow carrier resumed"'))
        self.assertLess(prepare.index("prepareClientEndpoint("), prepare.index('Info("tunnel: flow handoff prepared"'))
        self.assertIn('if err := exchangeResume(*attachment, req, deadline); err == nil {\n\t\t\t\treturn nil',recovery)
        self.assertLess(prepare.index("e.permit ="), prepare.index('Info("tunnel: flow handoff prepared"'))
        self.assertLess(final.index("check(bundle)"), final.index('Info("agent: image final cut"'))
        self.assertLess(final.index("CheckBundleCapabilities("), final.index('Info("agent: image final cut"'))
        resumed=resume.split('Info("tunnel: flow carrier resumed"',1)[1].split('return nil',1)[0]
        prepared=prepare.split('Info("tunnel: flow handoff prepared"',1)[1].split('}(e)',1)[0]
        self.assertEqual(set(__import__('re').findall(r'"([a-z_]+)"',resumed)),{'owner_generation','carrier_generation','mode'})
        self.assertEqual(set(__import__('re').findall(r'"([a-z_]+)"',prepared)),{'owner_generation','carrier_generation','generation'})
        self.assertIn('Info("agent: image final cut", "generation", cut.generation)',final)
        for text in (resume,prepare,final):
            for record in text.splitlines():
                if any(message in record for message in fixture.EVENTS.values()):
                    self.assertFalse(any(secret in record for secret in ("salt","nonce","ticket","target","key","Body")))


class TransportShellTests(unittest.TestCase):
    def test_installed_product_is_never_a_fixture_process(self):
        with mock.patch.object(peer.subprocess, 'Popen', side_effect=AssertionError('unexpected process creation')) as spawn:
            for command in ('tether', '/forbidden/tether'):
                with self.assertRaisesRegex(AssertionError, 'installed product execution forbidden'):
                    peer.run_supervised([command, 'exec', 'agt1'])
            spawn.assert_not_called()

    def test_deploy_adapter_uses_only_fake_processes_and_preserves_native_status(self):
        # Run the real shell adapter and shared log/provisioning owners. These
        # executable boundaries cannot fall through to an installed binary.
        with tempfile.TemporaryDirectory(prefix='s5-adapter-') as temp:
            root=Path(temp)
            shim='''#!/usr/bin/python3
import base64,json,os,pathlib,sys
root=pathlib.Path(os.environ['FLOW_CONTROL']); args=sys.argv[1:]
with (root/'calls').open('a') as out: out.write(json.dumps([pathlib.Path(sys.argv[0]).name]+args)+'\\n')
if (root/'mtu-failure').exists() and 'ip' in args and 'mtu' in args: sys.exit(7)
status=int((root/'status').read_text())
if status: sys.exit(status)
if pathlib.Path(sys.argv[0]).name=='sim-fake':
    online=not (root/'stopped').exists()
    if 'cluster' in args:
        path=root/'cluster-probes'; count=int(path.read_text())+1 if path.exists() else 1; path.write_text(str(count))
        print(json.dumps(dict(health='HEALTHY_HA',partial=False,errors=[],leader_id='brk2',nodes=[dict(phase='VOTER',reachable=True,applied_lag=1 if count==1 else 0,inconsistent=False) for _ in range(3)])))
    elif '--json' in args: print(json.dumps(dict(nodes=[dict(nid='agt1',status='ONLINE' if online else 'OFFLINE')])))
    else: print('agt1 '+('ONLINE' if online else 'OFFLINE'))
elif args[0]=='exec':
    if 'systemctl' in args and 'show' in args:
        print('start-limit-hit' if (root/'start-failure').exists() else 'success'); sys.exit(0)
    if 'systemctl' in args and 'start' in args and (root/'start-failure').exists():
        sys.exit(1)
    if 'systemctl' in args and 'stop' in args: (root/'stopped').touch()
    if 'systemctl' in args and ('start' in args or 'restart' in args): (root/'stopped').unlink(missing_ok=True)
    if '-i' in args:
        body=sys.stdin.buffer.read(); (root/'input-length').write_text(str(len(body)))
    if any(name in args for name in ('broker_slog','broker_panic','agent_slog','agent_panic')):
        raw=b'recorded log\\n'; print(json.dumps(dict(raw=base64.b64encode(raw).decode(),metadata=dict(read_ok=True,inode=7,length=len(raw)))))
elif args[0]=='inspect': print('127.0.0.1')
elif args[0]=='cp' and args[2].endswith('/agent.yaml'):
    (root/'policy-yaml').write_text(pathlib.Path(args[1]).read_text())
'''
            for name in ('docker-fake','sim-fake'):
                (root/name).write_text(shim); (root/name).chmod(0o700)
            (root/'status').write_text('0')
            args=SimpleNamespace(here=ROOT,instance='hermetic-control',sim=str(root/'sim-fake'))
            transport=fixture.Transport(args)
            # origin: hot-update S10 P11 — the transport now refuses to spawn without the shared supervisor adapter.
            with mock.patch.dict(os.environ,dict(PATH='/usr/bin:/bin',DOCKER=str(root/'docker-fake'),DOCKER_SUDO='0',FLOW_CONTROL=str(root),
                                                 SIM_UPGRADE_SUPERVISOR=os.environ['SIM_UPGRADE_SUPERVISOR']),clear=True):
                transport.run('policy')
                transport.run('observe-start', 'capacity-32', NONCE)
                self.assertEqual((root/'cluster-probes').read_text(), '2')
                (root/'mtu-failure').touch()
                with self.assertRaisesRegex(fixture.SetupError,'transport_native_rc_7'):
                    transport.run('observe-start','capacity-32',NONCE)
                (root/'mtu-failure').unlink()
                transport.run('exec','agt1','true')
                transport.run('exec-user','ctl1','true')
                transport.run('input','ctl1','cat',data=b'boundary-input')
                transport.run('copy-to',root/'status','agt1','/tmp/control')
                transport.run('copy-from','agt1','/tmp/control',root/'copy')
                self.assertEqual(transport.run('ip','agt1').strip(),b'127.0.0.1')
                for stream in ('broker_slog','broker_panic','agent_slog','agent_panic'):
                    transport.run('logs',stream,'brk1' if stream.startswith('broker') else 'agt1',root/'log.raw',root/'log.json')
                    self.assertTrue((root/'log.raw').is_file())
                    self.assertTrue((root/'log.json').is_file())
                    self.assertEqual((root/'log.raw').read_bytes(),b'recorded log\n')
                    self.assertTrue(json.loads((root/'log.json').read_text())['read_ok'])
                prior_calls=len((root/'calls').read_text().splitlines())
                (root/'start-failure').touch()
                with self.assertRaisesRegex(fixture.SetupError,'transport_native_rc_1_policy_provision_start_limit_hit'):
                    transport.run('policy')
                (root/'start-failure').unlink()
                fault_calls=[json.loads(line) for line in (root/'calls').read_text().splitlines()[prior_calls:]]
                self.assertEqual(sum('systemctl' in call and 'start' in call for call in fault_calls),1)
                self.assertEqual(sum('systemctl' in call and 'show' in call for call in fault_calls),1)
                self.assertFalse(any('reset-failed' in call for call in fault_calls))
                (root/'status').write_text('19')
                with self.assertRaisesRegex(fixture.SetupError,'transport_native_rc_19'):
                    transport.run('exec','agt1','true')
            calls=[json.loads(line) for line in (root/'calls').read_text().splitlines()]
            self.assertIn(['docker-fake','exec','-u','sim','sim-hermetic-control-ctl1','true'],calls)
            self.assertIn(['docker-fake','exec','-i','sim-hermetic-control-ctl1','cat'],calls)
            self.assertIn(['docker-fake','exec','sim-hermetic-control-agt1','ip','link','set','dev','lo','mtu','1500'],calls)
            checks=[row for row in calls if '/sys/class/net/lo/mtu' in ' '.join(row)]
            self.assertEqual(len(checks),1)
            self.assertIn('assert mtu==1500',checks[0][-1])
            self.assertTrue((root/'input-length').is_file())
            self.assertEqual(int((root/'input-length').read_text()),len(b'boundary-input'))
            self.assertTrue((root/'policy-yaml').is_file())
            self.assertIn('allow_private_destinations: true',(root/'policy-yaml').read_text())
            self.assertTrue(any('https://%s-artifact/' in item for row in calls for item in row))


class PeerTests(unittest.TestCase):
    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.C
    def test_empty_subscription_never_installs_or_exports_material(self):
        target=socket.inet_aton('127.0.0.1')+(9000).to_bytes(2,'big')
        # YAML is installed in the deploy image; this control supplies its
        # decoded public shape at that dependency boundary.
        with mock.patch.dict(sys.modules, {'yaml':SimpleNamespace(safe_load=json.loads)}), mock.patch.object(peer, 'supply_fd') as sealed, mock.patch.object(peer, 'call') as install:
            for body in (b'{"proxies":[]}', b'{"proxies":null}', b'{"proxies":[{"name":"another","type":"ss"}]}'):
                self.assertEqual(peer.install_supply(Path('/unused'),'bob',target+body),dict(rc=2,criterion='subscription_node_not_ready'))
            sealed.assert_not_called(); install.assert_not_called()
            with self.assertRaisesRegex(ValueError,'SS supply unavailable'):
                peer.install_supply(Path('/unused'),'bob',target+b'{"proxies":[{"name":"agt1","type":"ss","cipher":"invalid"}]}')

    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.A
    def test_opening_fault_mark_requires_progress_and_preserves_other_cut(self):
        opening = SimpleNamespace(sent=[0]*20, received=[0]*20, fault=3)
        survivor = SimpleNamespace(fault=7)
        owner = SimpleNamespace(flows=dict(opening=opening, bob=survivor))
        with mock.patch.object(peer.time, 'monotonic', return_value=42):
            self.assertEqual(peer.Peer.command(owner, dict(command='mark-opening'))['observed_at'], 42)
        self.assertEqual(opening.fault, 42)
        self.assertEqual(survivor.fault, 7)
        for direction in ('sent', 'received'):
            setattr(opening, direction, [0]*19)
            with self.assertRaisesRegex(ValueError, 'opening_duplex_samples_missing'):
                peer.Peer.command(owner, dict(command='mark-opening'))
            setattr(opening, direction, [0]*20)
        del owner.flows['opening']
        with self.assertRaisesRegex(ValueError, 'opening_duplex_samples_missing'):
            peer.Peer.command(owner, dict(command='mark-opening'))

    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.L
    def test_burst_target_uses_bounded_tcp_send_quantum(self):
        with tempfile.TemporaryDirectory() as temp:
            owner = peer.Peer(Path(temp), NONCE, 'target')
            listener = socket.socket(); listener.bind(('127.0.0.1',0)); listener.listen()
            client = socket.create_connection(listener.getsockname())
            server, _ = listener.accept()
            try:
                owner.specs['bob'] = dict(burst=True)
                client.sendall(b'bob\n')
                with mock.patch.object(peer, 'Flow') as flow:
                    owner.accept_one(server)
                    flow.return_value.run.assert_called_once()
                self.assertLessEqual(server.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF), 8192)
                self.assertEqual(server.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY), 1)
            finally:
                server.close(); client.close(); listener.close(); owner.server.close()

    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.R
    # Loopback's single large segment plus the bounded sender makes delayed
    # ACKs decide whether the real product ever receives enough pressure.
    def test_original_target_negotiates_ordinary_segment_size(self):
        with tempfile.TemporaryDirectory() as temp:
            owner = peer.Peer(Path(temp), NONCE, 'target')
            accepted = []
            try:
                owner.specs['bob'] = dict(burst=True)
                with mock.patch.object(peer, 'Flow') as flow:
                    flow.side_effect = lambda *args: accepted.append(args[2]) or mock.Mock()
                    owner.target()
                    with socket.create_connection(('127.0.0.1',owner.listener.getsockname()[1])) as client:
                        client.sendall(b'bob\n')
                        deadline = time.monotonic()+2
                        while not accepted and time.monotonic()<deadline:
                            time.sleep(.005)
                        self.assertEqual(len(accepted),1)
                        self.assertLessEqual(client.getsockopt(socket.IPPROTO_TCP,socket.TCP_MAXSEG),1200)
                        self.assertLessEqual(accepted[0].getsockopt(socket.IPPROTO_TCP,socket.TCP_MAXSEG),1200)
                        self.assertLessEqual(accepted[0].getsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF),8192)
            finally:
                owner.stop.set()
                for worker in owner.workers:
                    worker.join(timeout=3)
                    self.assertFalse(worker.is_alive())
                for stream in accepted: stream.close()
                for listener in (owner.listener,owner.open_listener):
                    if listener: listener.close()
                owner.server.close()

    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.A
    def test_failed_identity_keeps_exact_public_census_guard(self):
        with tempfile.TemporaryDirectory() as temp, socket.socket() as stream:
            owner = SimpleNamespace(root=Path(temp),nonce=NONCE,role='client')
            base = dict(pid=7,start_ticks=9,ino=11,app_nonce=NONCE)
            reader = mock.Mock(side_effect=[base,peer.probe.EvidenceError('original socket tuple unavailable'),base,
                                           PermissionError(13,'private-material'),peer.probe.EvidenceError('private-material')])
            flow = peer.Flow(owner,'ssh',stream,{},identity_reader=reader)
            flow.snapshot_identity();flow.snapshot_identity();flow.snapshot_identity();flow.snapshot_identity()
            self.assertEqual(flow.identities[1],dict(base,owner_lost=True,identity_failure=dict(kind='tuple_unavailable')))
            self.assertEqual(flow.identities[2],base)
            self.assertEqual(flow.identities[3],dict(base,owner_lost=True,identity_failure=dict(kind='os_error',errno=13)))
            self.assertEqual(flow.identities[4]['identity_failure'],dict(kind='identity_unavailable'))
            self.assertNotIn('private-material',json.dumps(flow.identities))

    # origin: hot-update S5 plan §9.2 / DRILL-RUN
    def test_terminal_marker_is_retained_before_product_removes_it(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'marker'
            owner = SimpleNamespace(stop=threading.Event())
            worker = threading.Thread(target=peer.watch_marker, args=(owner,path))
            worker.start()
            try:
                marker = dict(state='rolled_back',generation=3,upgrade_id='first',new_sha='candidate',prev_sha='original')
                peer.publish(path, marker)
                deadline = time.monotonic()+1
                while not hasattr(owner,'rollback_marker') and time.monotonic()<deadline: time.sleep(.001)
                self.assertEqual(owner.rollback_marker['marker'], marker)
                path.unlink()
                self.assertEqual(owner.rollback_marker['marker'], marker)
            finally:
                owner.stop.set();worker.join(1)
            self.assertFalse(worker.is_alive())

    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
    def test_open_accept_census_avoids_unrelated_byte_ledgers(self):
        flow = SimpleNamespace(accepts=1)
        owner = SimpleNamespace(flows={'opening': flow})
        value = peer.Peer.command(owner, dict(command='snapshot', accepts_only=True))
        self.assertEqual(value['flows'], {'opening': dict(target_accepts=1)})
        self.assertEqual(value['identity']['pid'], os.getpid())

    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
    def test_short_flow_closes_both_directions_after_exact_echo(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            owner = peer.Peer(root/'target', NONCE, 'target')
            owner.target()
            self.addCleanup(owner.server.close)
            self.addCleanup(owner.open_listener.close)
            data = b'predeclared short flow'
            owner.short_specs = {'short-between': data.hex()}
            client = SimpleNamespace()
            try:
                result = peer.Peer.command(client, dict(command='short-flow', name='short-between',
                    host='127.0.0.1', port=owner.listener.getsockname()[1], payload=data.hex()))
                self.assertEqual(result, dict(bytes=len(data), sha256=peer.hashlib.sha256(data).hexdigest()))
                self.assertEqual(owner.short_counts, {'short-between': 1})
                self.assertFalse(owner.flows)
            finally:
                owner.stop.set()
                for worker in owner.workers: worker.join(timeout=2)
                owner.listener.close()

    # origin: hot-update S5 plan §6.6 / DRILL-RUN
    def test_failed_observation_publishes_only_public_control_fields(self):
        row = dict(kind='ss-grant', source='agt1', at=100, values=[1,2,3],
                   data=b'private key', digest=b'private digest', nonce=b'private nonce')
        owner = SimpleNamespace(observations=SimpleNamespace(events=[row, dict(row, kind='ss-write')]))
        self.assertEqual(peer.public_observations(owner), [dict(kind='ss-grant', at=100, values=[1,2,3])])

    # origin: hot-update S5 plan §6.6 / §9.4 / DRILL-RUN
    def test_peer_errors_keep_public_class_and_discard_private_detail(self):
        for error, expected in ((ConnectionRefusedError("private target"), 'connection_refused'),
                                (ConnectionResetError("private target"), 'connection_reset'),
                                (BrokenPipeError("private target"), 'broken_pipe'),
                                (TimeoutError("private target"), 'socket_timeout'),
                                (ValueError("private key"), 'peer_observation_failed')):
            self.assertEqual(peer.public_error(error), expected)
            self.assertIn(peer.public_error(error), peer.PUBLIC_ERRORS)

    # origin: hot-update S5 plan §9.1 / T47 / DRILL-RUN. EOF follows
    # the final payload; recording a later completion would reverse that fact.
    def test_half_close_keeps_tail_completion_before_original_eof(self):
        with tempfile.TemporaryDirectory(prefix='s5-half-tail-') as directory:
            owner=SimpleNamespace(root=Path(directory),nonce=NONCE,role='target',stop=threading.Event(),finish=threading.Event(),deadline=time.monotonic()+5)
            listener=socket.socket();self.addCleanup(listener.close)
            listener.bind(('127.0.0.1',0));listener.listen(1)
            sender=socket.create_connection(listener.getsockname());receiver,_=listener.accept()
            self.addCleanup(sender.close);self.addCleanup(receiver.close)
            wanted=peer.probe.frame(NONCE,0,b'final original tail')
            sender.sendall(wanted);sender.shutdown(socket.SHUT_WR)
            flow=peer.Flow(owner,'half-close',receiver,dict(**{'class':'half-close'},up=dict(records=1)))
            flow.receive()
            self.assertEqual(len(flow.received),1)
            self.assertEqual([event['kind'] for event in flow.events],['eof'])
            self.assertLessEqual(flow.received[-1]['at'],flow.end)
            self.assertLessEqual(flow.end,flow.events[0]['at'])
            self.assertEqual((owner.root/'half-close.received').read_bytes(),wanted)

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Fast original senders
    # must keep their declared tails moving while other directions drain.
    def test_finite_tail_pacing_keeps_original_bytes_and_bounded_gaps(self):
        with tempfile.TemporaryDirectory(prefix='s5-tail-pacing-') as directory:
            owner=SimpleNamespace(root=Path(directory),nonce=NONCE,role='client',stop=threading.Event(),finish=threading.Event(),deadline=time.monotonic()+45)
            owner.finish.set()
            listener=socket.socket();self.addCleanup(listener.close)
            listener.bind(('127.0.0.1',0));listener.listen(1)
            sender=socket.create_connection(listener.getsockname());receiver,_=listener.accept()
            self.addCleanup(sender.close);self.addCleanup(receiver.close)
            spec=dict(size=32,protocol='raw',**{'class':'normal'},up=dict(records=200))
            flow=peer.Flow(owner,'original',sender,spec)
            clock=[1.0]
            with mock.patch.object(peer.time,'monotonic',side_effect=lambda:clock[0]), mock.patch.object(peer.time,'sleep',side_effect=lambda seconds:clock.__setitem__(0,clock[0]+seconds)):
                flow.send()
            self.assertEqual(len(flow.sent),200)
            self.assertGreater(flow.sent[-1]['at']-flow.sent[0]['at'],10)
            self.assertLessEqual(max(b['at']-a['at'] for a,b in zip(flow.sent,flow.sent[1:])),5)
            wanted=(owner.root/'original.sent').read_bytes()
            self.assertEqual(peer.exact(receiver,len(wanted)),wanted)
            self.assertEqual([row['seq'] for row in peer.probe.parse_frames(wanted,NONCE)],list(range(200)))

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Verify the configured
    # original receiver against actual ciphertext and kernel TCP reads.
    def test_original_cipher_receiver_advances_in_small_tcp_quanta(self):
        with tempfile.TemporaryDirectory(prefix='s5-cipher-quantum-') as directory:
            owner=peer.Peer(Path(directory),NONCE,'client')
            self.addCleanup(owner.server.close)
            listener=socket.socket();self.addCleanup(listener.close)
            listener.bind(('127.0.0.1',0));listener.listen(1)
            fd=peer.supply_fd(dict(password='control-password',server='127.0.0.1',port=listener.getsockname()[1]),socket.inet_aton('127.0.0.1')+(1234).to_bytes(2,'big'))
            self.addCleanup(os.close,fd);owner.keys['bob']=fd
            with mock.patch.object(peer.Flow,'run'):
                owner.connect(dict(id='bob',protocol='ss',key='bob',host='127.0.0.1',port=listener.getsockname()[1],burst=True))
            flow=owner.flows['bob']
            self.addCleanup(flow.stream.close)
            sender,_=listener.accept();self.addCleanup(sender.close)
            reference=peer.SS(sender,peer.probe.sealed_bytes(fd)[-32:])
            wanted=b'q'*16383
            reference.send(wanted)
            requested=[];original=flow.crypto.stream
            class Reader:
                def recv(self,size):
                    requested.append(size)
                    return original.recv(size)
            flow.crypto.stream=Reader()
            self.assertEqual(flow.crypto.receive(),wanted)
            self.assertGreater(len(requested),64)
            self.assertLessEqual(max(requested),256)

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Bound receiver pressure
    # below the normal stall limit and release it without changing the ledger.
    def test_receiver_pressure_is_bounded_and_released_for_finite_drain(self):
        for finishing in (False,True):
            with self.subTest(finishing=finishing), tempfile.TemporaryDirectory(prefix='s5-receiver-pressure-') as directory:
                owner=SimpleNamespace(root=Path(directory),nonce=NONCE,role='client',stop=threading.Event(),finish=threading.Event(),deadline=time.monotonic()+5)
                if finishing: owner.finish.set()
                listener=socket.socket();self.addCleanup(listener.close)
                listener.bind(('127.0.0.1',0));listener.listen(1)
                sender=socket.create_connection(listener.getsockname());receiver,_=listener.accept()
                self.addCleanup(sender.close);self.addCleanup(receiver.close)
                frames=[peer.probe.frame(NONCE,seq,b'q'*128) for seq in range(22)]
                sender.sendall(b''.join(frames));sender.shutdown(socket.SHUT_WR)
                remaining=iter(frames)
                def receive():
                    expected=next(remaining,None)
                    return peer.exact(receiver,len(expected)) if expected is not None else receiver.recv(1)
                crypto=SimpleNamespace(read_quantum=1024,receive=receive)
                flow=peer.Flow(owner,'bob',receiver,dict(burst=True,**{'class':'normal'},down=dict(records=22)),crypto)
                pauses=[]; started=time.monotonic()
                with mock.patch.object(peer.time,'sleep',side_effect=lambda seconds: pauses.append((seconds,len(flow.received)))):
                    flow.receive()
                self.assertEqual(len(flow.received),22)
                self.assertEqual((owner.root/'bob.received').read_bytes(),b''.join(frames))
                self.assertEqual([pause for pause in pauses if pause[0]>1],[] if finishing else [(3,18)])
                if not finishing:
                    self.assertGreaterEqual(crypto.pressure_until,started+4)
                    self.assertLessEqual(crypto.pressure_until,time.monotonic()+4)
                if finishing:
                    self.assertEqual(pauses,[])
                    self.assertEqual(crypto.read_quantum,0)

    # Real encrypted TCP bytes still decrypt when the pressure window expires
    # partway through a payload. A controlled clock models a stalled receiver
    # without spending the full artificial window in every control run.
    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.L
    def test_ss_read_pressure_expires_inside_original_cipher_record(self):
        listener=socket.socket();self.addCleanup(listener.close)
        listener.bind(('127.0.0.1',0));listener.listen(1)
        sender=socket.create_connection(listener.getsockname());receiver,_=listener.accept()
        self.addCleanup(sender.close);self.addCleanup(receiver.close)
        writer=peer.SS(sender,b'x'*32);clear=b'p'*8192
        writer.send(clear)
        clock=[1000.0];requested=[];pauses=[]
        class Reader:
            def recv(self,size):
                requested.append(size);clock[0]+=.5
                return receiver.recv(size)
        reader=peer.SS.__new__(peer.SS)
        reader.stream=Reader();reader.master=b'x'*32;reader.read_quantum=128
        reader.pressure_until=1004;reader.rx_key=None;reader.rx=0;reader.down=bytearray()
        def pause(seconds):
            pauses.append(clock[0]);clock[0]+=seconds
        with mock.patch.object(peer.time,'monotonic',side_effect=lambda: clock[0]),mock.patch.object(peer.time,'sleep',side_effect=pause):
            self.assertEqual(reader.receive(),clear)
        self.assertIn(128,requested)
        self.assertGreater(max(requested),128)
        self.assertTrue(pauses)
        self.assertTrue(all(at<reader.pressure_until for at in pauses))
        self.assertEqual(reader.read_quantum,128)
        self.assertEqual(bytes(reader.down),bytes(writer.up))

    # origin: hot-update S5 plan §9.4 / DRILL-RUN. A failed oracle
    # stays native rc2 and exposes only its fixed public criterion.
    def test_peer_retains_failed_observer_criterion(self):
        with tempfile.TemporaryDirectory(prefix="s5-peer-failure-") as directory:
            root = Path(directory)
            owner = peer.Peer(root, NONCE, "client")
            errors = []
            def command(request, rights=()):
                if request["command"] == "cleanup":
                    owner.stop.set()
                    return dict(cleaned=True)
                raise peer.observation.CustodyError("opening_fd_changed")
            owner.command = command
            def exercise():
                try:
                    owner.run()
                except BaseException as error:
                    errors.append(error)
            thread = threading.Thread(target=exercise, daemon=True)
            thread.start()
            try:
                self.assertEqual(peer.call(root, dict(command="product-receipt")),
                                 dict(rc=2, criterion="opening_fd_changed", public_observations=[]))
            finally:
                peer.call(root, dict(command="cleanup"))
                thread.join(timeout=3)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Exercise TCP reads
    # directly so receiver pressure cannot be an invented write receipt.
    def test_cipher_receiver_frees_tcp_capacity_in_declared_quanta(self):
        listener = socket.socket()
        self.addCleanup(listener.close)
        listener.bind(("127.0.0.1", 0)); listener.listen(1)
        sender = socket.create_connection(listener.getsockname())
        receiver, _ = listener.accept()
        self.addCleanup(sender.close); self.addCleanup(receiver.close)
        requested = []
        class Reader:
            def recv(self, size):
                requested.append(size)
                return receiver.recv(size)
        sender.sendall(b"q"*4097)
        with mock.patch.object(peer.time, "sleep") as pause:
            self.assertEqual(peer.exact(Reader(), 4097, 1024), b"q"*4097)
            self.assertEqual(max(requested), 1024)
            self.assertEqual(pause.call_count, len(requested))
            self.assertTrue(all(call.args == (.001,) for call in pause.call_args_list))

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. The finite workload
    # must exceed TCP GSO while retaining exact independent declarations.
    def test_ss_burst_batches_declared_frames_and_keeps_small_finite_tail(self):
        with tempfile.TemporaryDirectory(prefix="s5-burst-") as temp:
            owner = SimpleNamespace(root=Path(temp), nonce=NONCE, role="client", stop=threading.Event(), finish=threading.Event(), deadline=time.monotonic()+5)
            owner.finish.set()
            listener = socket.socket()
            self.addCleanup(listener.close)
            listener.bind(("127.0.0.1", 0)); listener.listen(1)
            sender = socket.create_connection(listener.getsockname())
            receiver, _ = listener.accept()
            self.addCleanup(sender.close); self.addCleanup(receiver.close)
            wire = []
            crypto = mock.Mock()
            crypto.send.side_effect = lambda data: wire.append(bytes(data))
            flow = peer.Flow(owner, "bob", sender, dict(size=32768, burst=True, protocol="ss", **{"class":"normal"}, up=dict(records=540)), crypto)
            flow.send()
            self.assertEqual(len(flow.sent), 540)
            self.assertTrue(flow.tx_done.is_set())
            self.assertEqual(len(wire), 20+512//4+8)
            self.assertTrue(all(len(row) == 4*(32768+peer.probe.HEADER.size) for row in wire[20:-8]))
            self.assertTrue(all(len(row) == 128+peer.probe.HEADER.size for row in wire[:20]+wire[-8:]))
            raw = (Path(temp)/"bob.sent").read_bytes()
            expected = peer.declaration("bob.up", NONCE, 540, 32768, burst=True)
            self.assertEqual(len(raw), expected["bytes"])
            self.assertEqual(peer.probe.digest(raw), expected["sha256"])
            frames = peer.probe.parse_frames(raw, NONCE)
            self.assertEqual(len(frames), 540)
            self.assertEqual([row["length"] for row in frames[:20]+frames[-8:]], [128]*28)

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. TCP skb sizing differs
    # from AF_UNIX and is the actual deployment transport.
    def test_ss_tcp_backpressure_records_original_cipher_suffix(self):
        listener = socket.socket()
        self.addCleanup(listener.close)
        listener.bind(("127.0.0.1", 0)); listener.listen(1)
        sender = peer.connect_socket(*listener.getsockname(), burst=True)
        receiver, _ = listener.accept()
        self.addCleanup(sender.close); self.addCleanup(receiver.close)
        self.assertLessEqual(sender.getsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG), 1200)
        self.assertLessEqual(receiver.getsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG), 1200)
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        receiver.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        crypto = peer.SS(sender, b"x"*32)
        captured = bytearray()
        def read():
            while True:
                part = receiver.recv(2048)
                if not part: return
                captured.extend(part)
                time.sleep(.002)
        task = threading.Thread(target=read); task.start()
        try:
            crypto.send(b"p"*65536)
            sender.shutdown(socket.SHUT_WR)
            task.join(timeout=5)
            self.assertFalse(task.is_alive())
            self.assertTrue(crypto.partials)
            self.assertTrue(all(row["seal_calls"] == 1 for row in crypto.partials))
            self.assertEqual(bytes(captured), bytes(crypto.up))
            self.assertEqual(peer.probe.decrypt_ss(bytes(captured), b"x"*32)[0], b"p"*65536)
        finally:
            sender.close(); receiver.close(); task.join(timeout=2)

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Checkpoint requests
    # contain growing ledgers and SCM_RIGHTS must stay on the first packet.
    def test_peer_accepts_large_checkpoint_request_and_one_sealed_descriptor(self):
        with tempfile.TemporaryDirectory(prefix="s5-large-rpc-") as temp:
            owner = peer.Peer(Path(temp), NONCE, "client")
            original = owner.command
            data = "z" * (400 << 10)
            observed = []
            def command(request, rights=()):
                if request["command"] == "large":
                    observed.append((request["data"], peer.probe.sealed_bytes(rights[0])))
                    os.close(rights[0])
                    return dict(ok=True)
                return original(request, rights)
            owner.command = command
            errors = []
            def exercise():
                try: owner.run()
                except BaseException as exc: errors.append(exc)
            task = threading.Thread(target=exercise)
            task.start()
            fd = peer.sealed(b"private-control")
            try:
                self.assertEqual(peer.call(Path(temp), dict(command="large", data=data), fd), dict(ok=True))
                self.assertEqual(observed, [(data, b"private-control")])
            finally:
                os.close(fd)
                if task.is_alive(): peer.call(Path(temp), dict(command="cleanup"))
                task.join(timeout=2)
                self.assertFalse(task.is_alive())
                self.assertEqual(errors, [])

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Use an actual socket,
    # not a scripted n, to prove the workload exposes successful short writes.
    def test_ss_backpressure_records_real_short_write_without_reseal(self):
        sender, receiver = socket.socketpair()
        self.addCleanup(sender.close)
        self.addCleanup(receiver.close)
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        crypto = peer.SS(sender, b"x"*32)
        raw = bytearray()
        stopped = threading.Event()
        def read():
            while not stopped.is_set():
                part = receiver.recv(2048)
                if not part:
                    return
                raw.extend(part)
                time.sleep(.002)
        thread = threading.Thread(target=read)
        thread.start()
        try:
            crypto.send(b"p"*65536)
            deadline = time.monotonic()+2
            while len(raw) < len(crypto.up) and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue(crypto.partials)
            self.assertEqual(len(crypto.calls), 10)
            self.assertTrue(all(row["seal_calls"] == 1 for row in crypto.partials))
            self.assertEqual(bytes(raw), bytes(crypto.up))
            self.assertEqual(peer.probe.decrypt_ss(bytes(raw), b"x"*32)[0], b"p"*65536)
        finally:
            stopped.set()
            sender.shutdown(socket.SHUT_WR)
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Duplex ledgers grow
    # past one Unix seqpacket while the original keeper must remain alive.
    def test_peer_snapshot_fragments_large_ledger_without_losing_keeper(self):
        with tempfile.TemporaryDirectory(prefix="s5-peer-response-") as directory:
            root = Path(directory)
            owner = peer.Peer(root, NONCE, "client")
            document = dict(records="x" * (400 << 10))
            errors = []
            def command(request, rights=()):
                if request["command"] == "cleanup":
                    owner.stop.set()
                    return dict(cleaned=True)
                return document
            owner.command = command
            def exercise():
                try:
                    owner.run()
                except BaseException as exc:
                    errors.append(exc)
            thread = threading.Thread(target=exercise, daemon=True)
            thread.start()
            try:
                self.assertEqual(peer.call(root, dict(command="snapshot")), document)
                self.assertEqual(peer.call(root, dict(command="cleanup")), dict(cleaned=True))
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])
            finally:
                owner.stop.set()

    def test_private_target_and_master_share_sealed_supply_descriptor(self):
        target=socket.inet_aton('127.0.0.1')+(1234).to_bytes(2,'big')
        fd=peer.supply_fd(dict(password='control-password',server='brk1',port=14001),target)
        try:
            raw=peer.probe.sealed_bytes(fd)
            self.assertEqual(raw[-38:-32],target)
            first=__import__('hashlib').md5(b'control-password').digest()
            self.assertEqual(raw[-32:],first+__import__('hashlib').md5(first+b'control-password').digest())
            with self.assertRaises(OSError): os.write(fd,b'change')
        finally:
            os.close(fd)
        with self.assertRaisesRegex(ValueError,'invalid private supply'):
            peer.supply_fd(dict(password='control-password',server='brk1',port=14001),b'')

    def test_ssh_process_boundary_keeps_one_process_socket_and_channel(self):
        # Fake OpenSSH owns a real TCP socket and exchanges independent frames
        # on stdio. /usr/bin/ssh is never executed by this control.
        with tempfile.TemporaryDirectory(prefix='s5-ssh-control-') as temp:
            root=Path(temp); script=root/'ssh-boundary.py'
            script.write_text('''import importlib.util,os,socket,sys,time
from pathlib import Path
spec=importlib.util.spec_from_file_location("peer",sys.argv[1]); peer=importlib.util.module_from_spec(spec); spec.loader.exec_module(peer)
nonce=sys.argv[2]
listener=socket.socket(); listener.bind(("127.0.0.1",0)); listener.listen(1)
client=socket.create_connection(listener.getsockname()); target,_=listener.accept()
def exact(n):
    data=bytearray()
    while len(data)<n:
        chunk=os.read(0,n-len(data))
        if not chunk: raise SystemExit(0)
        data.extend(chunk)
    return bytes(data)
for seq in range(12):
    header=exact(peer.probe.HEADER.size); length=peer.probe.HEADER.unpack(header)[3]; exact(length)
    os.write(1,peer.probe.frame(nonce,seq,peer.payload("ssh.down",seq,16)))
while os.read(0,1): pass
''')
            keeper=peer.Peer(root/'client',NONCE,'client')
            row=dict(id='ssh',protocol='raw',**{'class':'normal'},size=16,host='brk1',port=14001,
                up=peer.declaration('ssh.up',NONCE,12,16),down=peer.declaration('ssh.down',NONCE,12,16))
            keeper.finish.set()
            try:
                command=[sys.executable,'-I','-B',str(script),str(LIB/'flow_fixture_peer.py'),NONCE]
                with mock.patch.object(peer,'ssh_command',return_value=command) as boundary:
                    keeper.connect(row)
                    flow=keeper.flows['ssh']
                    self.assertTrue(flow.done.wait(5))
                    self.assertTrue(flow.tx_done.wait(5))
                    photo=flow.snapshot()
                    self.assertEqual(photo['identities'][0],photo['identities'][-1])
                    self.assertEqual(len(photo['received']),12)
                    self.assertEqual(len(photo['sent']),12)
                    with self.assertRaisesRegex(ValueError,'application redial'): keeper.connect(row)
                    self.assertEqual(boundary.call_count,1)
            finally:
                keeper.command(dict(command='cleanup'))
                for flow in keeper.flows.values():
                    for thread in flow.threads: thread.join(timeout=3)
                keeper.server.close()

    def test_ssh_ancestor_walk_stops_at_pid_one(self):
        with self.assertRaisesRegex(peer.probe.EvidenceError,'original SSH socket unavailable'):
            peer.tcp_owner(1,NONCE,ancestors=True)
        command=peer.ssh_command(dict(host='brk1',port=14001),'/tmp/fixture',NONCE)
        self.assertEqual(command[0],'/usr/bin/ssh')
        self.assertIn('NumberOfPasswordPrompts=1',command)
        self.assertIn('stdio',command)

    def test_ssh_target_stdio_records_independent_duplex_bytes(self):
        with tempfile.TemporaryDirectory(prefix='s5-ssh-stdio-') as temp:
            root=Path(temp)
            row=dict(id='ssh',**{'class':'normal'},size=16,
                up=peer.declaration('ssh.up',NONCE,3,16),down=peer.declaration('ssh.down',NONCE,3,16))
            peer.publish(root/'ssh-spec.json',row); (root/'ssh-finish').touch()
            child=subprocess.Popen([sys.executable,'-I','-B',str(LIB/'flow_fixture_peer.py'),'stdio','--root',str(root),'--nonce',NONCE],
                                   stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                raw=b''.join(peer.probe.frame(NONCE,seq,peer.payload('ssh.up',seq,16)) for seq in range(3))
                child.stdin.write(raw); child.stdin.close(); child.stdin=None
                received=bytearray()
                while len(received)<row['down']['bytes']:
                    ready,_,_=__import__('select').select([child.stdout],[],[],5)
                    self.assertTrue(ready)
                    chunk=os.read(child.stdout.fileno(),row['down']['bytes']-len(received))
                    self.assertTrue(chunk); received.extend(chunk)
                deadline=time.monotonic()+5
                while not (root/'ssh.received').exists() or (root/'ssh.received').read_bytes()!=raw:
                    self.assertLess(time.monotonic(),deadline); time.sleep(.01)
                self.assertEqual(peer.probe.digest(received),row['down']['sha256'])
                self.assertEqual((root/'ssh.received').read_bytes(),raw)
                (root/'ssh-cleanup').touch()
                out,err=child.communicate(timeout=5)
                self.assertEqual(child.returncode,0,err.decode())
            finally:
                if child.poll() is None:
                    child.kill(); child.communicate()

    def test_sealed_custody_survives_judge_subprocess_and_rejects_plain_fd(self):
        fd = peer.sealed(b"private-custody")
        try:
            code = 'import fcntl,os,sys; f=int(sys.argv[1]); assert fcntl.fcntl(f,1034)&15==15; assert os.pread(f,32,0)==b"private-custody"'
            for _ in range(3):
                self.assertEqual(subprocess.run([sys.executable,"-I","-B","-c",code,str(fd)],pass_fds=(fd,)).returncode,0)
            with self.assertRaises(OSError): os.write(fd,b"change")
        finally:
            os.close(fd)
        with tempfile.TemporaryFile() as plain:
            with self.assertRaises((OSError,peer.probe.EvidenceError)): peer.probe.sealed_bytes(plain.fileno())

    def test_client_records_actual_seals_before_write_failure(self):
        class BrokenSocket:
            def sendall(self, data): pass
            def send(self, data): raise OSError("write failed")
        cipher = peer.SS(BrokenSocket(),b"x"*32)
        with self.assertRaises(OSError): cipher.send(b"payload")
        self.assertEqual(len(cipher.calls),2)
        self.assertEqual([row["nonce"] for row in cipher.calls],["00"*12,"01"+"00"*11])
        self.assertEqual(peer.probe.decrypt_ss(bytes(cipher.up),b"x"*32)[0],b"payload")

    def test_partial_cipher_receipt_uses_the_actual_writer_count(self):
        class ShortSocket:
            def sendall(self,data): pass
            def send(self,data): return min(3,len(data))
        cipher=peer.SS(ShortSocket(),b"x"*32)
        cipher.send(b"payload")
        self.assertTrue(cipher.partials)
        self.assertTrue(all(row['written']==3 and row['suffix_bytes']==row['attempt_len']-3 for row in cipher.partials))
        self.assertTrue(all(row['seal_calls']==1 for row in cipher.partials))
        self.assertEqual(len(cipher.calls),2)

    def test_receiver_observes_duplicate_tail_after_declared_total(self):
        with tempfile.TemporaryDirectory(prefix='s5-tail-') as temp:
            root=Path(temp)
            owner=SimpleNamespace(root=root,nonce=NONCE,role='target',stop=__import__('threading').Event(),
                                  finish=__import__('threading').Event(),deadline=time.monotonic()+5)
            listener=socket.socket(); listener.bind(('127.0.0.1',0)); listener.listen(1)
            client=socket.create_connection(listener.getsockname()); target,_=listener.accept()
            self.addCleanup(listener.close); self.addCleanup(client.close); self.addCleanup(target.close)
            flow=peer.Flow(owner,'tail',target,dict(protocol='raw',**{'class':'normal'},size=32,up={'records':1},down={'records':1}))
            task=__import__('threading').Thread(target=flow.receive); task.start()
            raw=peer.probe.frame(NONCE,0,b'tail')
            client.sendall(raw)
            self.assertTrue(flow.done.wait(1))
            client.sendall(raw)
            deadline=time.monotonic()+1
            while len(flow.received)<2 and time.monotonic()<deadline: time.sleep(.01)
            client.shutdown(socket.SHUT_WR)
            task.join(timeout=2)
            self.assertFalse(task.is_alive())
            self.assertEqual(len(flow.received),2)
            self.assertEqual((root/'tail.received').read_bytes(),raw+raw)

    def test_common_judge_keeps_native_loss_and_refuses_client_only_crypto_proof(self):
        spec=importlib.util.spec_from_file_location('oracle_controls',ROOT/'tests/hot-update-flow-probe-test.py')
        controls=importlib.util.module_from_spec(spec); spec.loader.exec_module(controls)
        sensor=controls.FlowProbeTests(); sensor.setUp()
        self.addCleanup(sensor.doCleanups)
        sensor.save()
        keeper=peer.Peer(sensor.root,controls.NONCE,'client')
        self.addCleanup(keeper.server.close)
        self.addCleanup(lambda: [os.close(fd) for fd in keeper.private_fds])
        result=keeper.command(dict(command='judge',carrier_transition='replaced'))
        self.assertEqual((result['rc'],result['criterion']),(2,'domain_observations_unavailable'))
        keeper.flows['bob']=SimpleNamespace(crypto=SimpleNamespace(up=b'cipher',down=b'cipher',calls=[]))
        result=keeper.command(dict(command='judge',carrier_transition='replaced'))
        self.assertEqual((result['rc'],result['criterion']),(2,'product_seal_observer_unavailable'))
        sensor.document['flows'][0]['carrier_before']='';sensor.document['flows'][0]['carrier_after']='';sensor.save()
        (sensor.root/'up.received').write_bytes(b'')
        result=keeper.command(dict(command='judge',carrier_transition='replaced'))
        self.assertEqual(result['rc'],1)
        self.assertEqual(result['verdict'],'ASSERT-FAIL')
        self.assertEqual(result['criterion'],'final_totals')

    def test_live_peer_keeps_original_socket_through_finish_and_never_redials(self):
        with tempfile.TemporaryDirectory(prefix="s5-peer-") as temp:
            root = Path(temp); target,client=root/"target",root/"client"
            processes=[]
            try:
                for role,path in (("target",target),("client",client)):
                    processes.append(subprocess.Popen([sys.executable,"-I","-B",str(LIB/"flow_fixture_peer.py"),"serve",
                        "--root",str(path),"--nonce",NONCE,"--role",role],stdout=subprocess.PIPE,stderr=subprocess.PIPE))
                deadline=time.monotonic()+5
                while not all((path/"ready.json").exists() for path in (target,client)):
                    self.assertLess(time.monotonic(),deadline); time.sleep(.01)
                port=json.loads((target/"ready.json").read_text())["port"]
                row=dict(id="original",protocol="raw",**{"class":"normal"},size=32,host="127.0.0.1",port=port,
                         up=peer.declaration("original.up",NONCE,48,32),down=peer.declaration("original.down",NONCE,48,32))
                peer.call(target,dict(command="configure",flows=[row]))
                self.assertTrue(peer.call(client,dict(command="start",flows=[row]))["started"])
                while True:
                    before=peer.call(client,dict(command="snapshot"))
                    if len(before["flows"]["original"]["received"])>=2: break
                    self.assertLess(time.monotonic(),deadline); time.sleep(.01)
                peer.call(client,dict(command="mark")); peer.call(target,dict(command="mark"))
                duplicate=peer.call(client,dict(command="open",flow=row))
                self.assertEqual(duplicate["rc"],2)
                final=peer.call(client,dict(command="finish"))
                peer.call(target,dict(command="finish"))
                after=final["flows"]["original"]
                observed=peer.call(client,dict(command="snapshot"))["flows"]["original"]
                self.assertEqual(observed['window']['end'],after['window']['end'])
                self.assertEqual(observed['events'],after['events'])
                self.assertEqual(after["identities"][0],after["identities"][-1])
                self.assertEqual(len(after["sent"]),48)
                self.assertEqual(len(after["received"]),48)
                self.assertFalse(after["events"])
                self.assertEqual((client/"original.sent").read_bytes(),(target/"original.received").read_bytes())
                self.assertEqual((target/"original.sent").read_bytes(),(client/"original.received").read_bytes())
            finally:
                for path in (client,target):
                    if (path/"peer.sock").exists():
                        peer.call(path,dict(command="cleanup"))
                for process in processes:
                    try: out,err=process.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill(); out,err=process.communicate()
                        self.fail("peer leaked after cleanup: "+err.decode())
                    self.assertEqual(process.returncode,0,err.decode())


# origin: hot-update S5 plan §9.1 / DRILL-OBSERVER. These controls drive the
# shipping collector against SCM_RIGHTS records emitted by the real Go observer
# test binary. Synthetic declared state is confined to sealed descriptors.
# origin: hot-update S5 plan §6.6 / §9.4 / DRILL-RUN 75.C.
class CustodyFailureDiagnosticTests(unittest.TestCase):
    def test_fetch_poison_accepts_only_fixed_public_diagnostics(self):
        valid=dict(source='agt1',criterion='custody_header',values=[0,0,0])
        for field,value in ((None,None),('source','brk3'),('criterion','private material'),('values',[b'private',0,0])):
            with self.subTest(field=field):
                failure=valid.copy()
                if field:failure[field]=value
                records=peer.observation.Records()
                reply=json.dumps(dict(broken=True,failure=failure),default=lambda _: 'private').encode()
                with mock.patch.object(peer.observation.socket,'socket'),mock.patch.object(peer.observation,'receive',return_value=(reply,[])):
                    with self.assertRaises(peer.observation.CustodyError):
                        peer.observation.fetch(records,Path('/unused'),'agt1')
                self.assertTrue(records.broken)
                self.assertEqual(records.failure,valid if field is None else None)

    def test_real_keeper_propagates_header_and_producer_poison(self):
        for mode in ('header','producer'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory(prefix='s5-keeper-poison-') as directory:
                root=Path(directory);producer,consumer=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
                keeper=subprocess.Popen([sys.executable,'-I','-B',str(LIB/'flow_observation.py'),'keeper','--root',str(root),'--node','brk3','--fd',str(consumer.fileno())],pass_fds=(consumer.fileno(),),stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
                consumer.close();errors=[]
                def supply():
                    try:
                        if mode=='header':
                            fd=peer.sealed(b'invalid private batch')
                            try:producer.sendmsg([b'TDC1'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,peer.array.array('i',[fd]))])
                            finally:os.close(fd)
                            # Later poison must not erase the first fixed guard.
                            producer.send(b'TDBR')
                        else:
                            self.assertEqual(producer.recv(4),b'TDP1');producer.send(b'TDBR')
                    except BaseException as error:errors.append(error)
                thread=threading.Thread(target=supply);thread.start();records=peer.observation.Records()
                try:
                    deadline=time.monotonic()+5
                    while not (root/'brk3.sock').exists():
                        self.assertLess(time.monotonic(),deadline);time.sleep(.01)
                    with self.assertRaises(peer.observation.CustodyError):peer.observation.fetch(records,root,'brk3')
                    expected='custody_header' if mode=='header' else 'custody_producer_poison'
                    self.assertEqual(records.failure,dict(source='brk3',criterion=expected,values=[0,0,0]))
                    self.assertEqual(errors,[])
                finally:
                    records.close();producer.close();thread.join(timeout=5)
                    keeper.terminate();keeper.communicate(timeout=5)
                    self.assertFalse(thread.is_alive())

    # The producer keeps emitting while a judge copies its first page. Slow
    # immutable clones expose the real socket backpressure without relaxing the
    # producer queue, custody quotas, or fresh-barrier deadline.
    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.C
    def test_keeper_drains_producer_during_page_cloning(self):
        with tempfile.TemporaryDirectory(prefix='s5-keeper-concurrent-') as directory:
            root=Path(directory)
            producer,consumer=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
            signal,notification=socket.socketpair()
            script="""import importlib.util,sys,time,socket,threading
spec=importlib.util.spec_from_file_location('observation',sys.argv[1])
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
original=module.sealed;signal=socket.socket(fileno=int(sys.argv[4]));announced=False
def slow_clone(data):
    global announced
    if not announced:
        signal.sendall(b'C');announced=True
        # A slow immutable copy must not stop the independent intake.
        time.sleep(.3)
    time.sleep(.005)
    return original(data)
module.sealed=slow_clone
original_decode=module.decode;delayed_decode=False
def expensive_judge_decode(fd, retain_events=True, runtime_samples=None):
    global delayed_decode
    # A declared semantic-decoding stall belongs to the downstream judge;
    # frame intake must validate and retain immutable bytes independently.
    if announced and not delayed_decode and retain_events and threading.current_thread().name=='custody-intake':
        delayed_decode=True
        time.sleep(.3)
    if retain_events:
        return original_decode(fd, runtime_samples=runtime_samples)
    return original_decode(fd, retain_events=False, runtime_samples=runtime_samples)
module.decode=expensive_judge_decode
module.keeper(module.Path(sys.argv[2]),'agt1',int(sys.argv[3]))
"""
            keeper=subprocess.Popen([sys.executable,'-I','-B','-c',script,str(LIB/'flow_observation.py'),str(root),str(consumer.fileno()),str(signal.fileno())],pass_fds=(consumer.fileno(),signal.fileno()),stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            consumer.close();signal.close();errors=[]
            def send(sequence,nonblocking=False):
                raw=b'TDRILL1\0'+bytes([1])*32+bytes([2])*16+struct.pack('<QQI',sequence,0,0)
                fd=peer.sealed(raw)
                try:producer.sendmsg([b'TDC1'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,peer.array.array('i',[fd]))],socket.MSG_DONTWAIT if nonblocking else 0)
                finally:os.close(fd)
            def supply():
                try:
                    for sequence in range(1,128):send(sequence)
                    self.assertEqual(producer.recv(4),b'TDP1');producer.send(b'TDOK')
                    self.assertEqual(notification.recv(1),b'C')
                    producer.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,16384)
                    for sequence in range(128,728):
                        send(sequence,True);time.sleep(.002)
                    while producer.recv(4):producer.send(b'TDOK')
                except OSError as error:
                    if not isinstance(error,(BrokenPipeError,ConnectionResetError)):errors.append(error)
                except BaseException as error:errors.append(error)
            thread=threading.Thread(target=supply);thread.start();records=peer.observation.Records()
            try:
                deadline=time.monotonic()+5
                while not (root/'agt1.sock').exists():
                    self.assertLess(time.monotonic(),deadline);time.sleep(.01)
                peer.observation.fetch(records,root,'agt1')
                self.assertEqual(errors,[], 'page cloning stopped draining the producer')
                self.assertEqual(len(records.frames),727)
                for sequence,(fd,offset,length) in enumerate(records.frames,1):
                    self.assertEqual(os.pread(fd,length,offset)[56:64],struct.pack('<Q',sequence))
            finally:
                records.close();producer.shutdown(socket.SHUT_RDWR);producer.close();notification.shutdown(socket.SHUT_RDWR);notification.close()
                thread.join(timeout=5);keeper.terminate();keeper.communicate(timeout=5)
                self.assertFalse(thread.is_alive())

    def test_poison_reason_is_public_and_never_allows_rows(self):
        # The real producer's fixed cause occupies the original poison word;
        # it never creates a second public channel for event data or errors.
        # origin: hot-update S5 plan §6.6 / §9.4 / DRILL-RUN 75.C
        for cause,criterion in peer.observation.PRODUCER_FAILURES.items():
            with self.subTest(cause=cause):
                raw=b'TDRILL1\0'+bytes([1])*32+bytes([2])*16+struct.pack('<QQI',1,cause,0)
                poisoned=peer.observation.Records()
                try:
                    with self.assertRaises(peer.observation.CustodyError):
                        poisoned.add(peer.sealed(raw),'agt1')
                    self.assertTrue(poisoned.broken)
                    self.assertEqual(poisoned.failure,dict(source='agt1',criterion=criterion,values=[0,0,0]))
                    self.assertEqual(peer.public_observations(SimpleNamespace(observations=poisoned)),
                        [dict(kind='custody-failure',source='agt1',criterion=criterion,values=[0,0,0])])
                finally:poisoned.close()
        records=peer.observation.Records()
        self.addCleanup(records.close)
        fd=peer.sealed(b'invalid private batch')
        with self.assertRaises(peer.observation.CustodyError):
            records.add(fd,'brk3')
        self.assertTrue(records.broken)
        with self.assertRaises(peer.observation.CustodyError):
            records.rows('flow-state')
        result=peer.public_observations(SimpleNamespace(observations=records))
        self.assertEqual(result,[dict(kind='custody-failure',source='brk3',criterion='custody_header',values=[0,0,0])])
        for field,value in (('source','private target'),('criterion','private material'),('values',[b'private state',0,0])):
            original=records.failure.copy();records.failure[field]=value
            self.assertEqual(peer.public_observations(SimpleNamespace(observations=records)),[])
            records.failure=original

class ObservationCustodyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.build=tempfile.TemporaryDirectory(prefix='s5-observer-binary-')
        cls.binary=Path(cls.build.name)/'custody.test'
        result=subprocess.run(['go','test','-c','-o',str(cls.binary),'./internal/drillobserve'],
                              cwd=REPO,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        if result.returncode:
            raise RuntimeError('Go observer test build failed: '+result.stderr.decode())

    @classmethod
    def tearDownClass(cls):
        cls.build.cleanup()

    def recording(self,mode='normal'):
        observation=peer.observation
        records=observation.Records()
        self.addCleanup(records.close)
        for source in ('agt1','brk1'):
            producer,receiver=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
            config=peer.sealed(b'TDCFG1\0\0'+struct.pack('<Q',producer.fileno())+os.urandom(16))
            try:
                env={k:v for k,v in os.environ.items() if not k.startswith('TETHER_TEST_DRILL_')}
                env.update(TETHER_TEST_DRILL_OBSERVER='v1',TETHER_TEST_DRILL_CUSTODY_FD=str(config),
                           TETHER_TEST_OBSERVER_RECORDING=mode)
                child=subprocess.Popen([str(self.binary),'-test.run=^TestCustodyRecordedStream$','-test.count=1'],
                    pass_fds=(config,producer.fileno()),env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                producer.close();receiver.settimeout(10)
                while True:
                    raw,fds=observation.receive(receiver)
                    if not raw:
                        break
                    self.assertEqual(raw,b'TDC1')
                    self.assertEqual(len(fds),1)
                    records.add(fds[0],source)
                out,err=child.communicate(timeout=10)
                self.assertEqual(child.returncode,0,err.decode())
                self.assertNotIn(b'SKIP',out)
            finally:
                producer.close();receiver.close();os.close(config)
        return records

    def context(self,records):
        key=bytes([6])*32
        alice=SimpleNamespace(crypto=SimpleNamespace(rx_key=key))
        bob=SimpleNamespace(crypto=SimpleNamespace(rx_key=key),received=[dict(at=time.monotonic()+1) for _ in range(21)])
        return SimpleNamespace(nonce=NONCE,observations=records,observation_checkpoints={},
                               flows={'alice':alice,'bob':bob})

    def request(self,label):
        value=kernel();value['connections']=[dict(state='02',ino=77)]
        value['fds']=[dict(dev=1,ino=77,fd=8)]
        return dict(label=label,kernel=value,broker_kernel=value,target=dict(flows={}),profile='normal')

    def consume(self,owner,label,request=None):
        with mock.patch.object(peer.observation,'fetch',return_value=owner.observations):
            return peer.observation.product_receipt(owner,label,request or self.request(label))

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Readiness consumes
    # each original's client count and matching product payload primitive.
    def test_partial_readiness_requires_every_original_and_both_writers(self):
        records = self.recording()
        owner = self.context(records)
        for flow in owner.flows.values(): flow.crypto.partials = [dict(written=7)]
        write = records.rows('ss-write', 'agt1')[0]
        write['values'][0], write['values'][2], write['values'][6] = 36, 29, 36
        write['data'] = bytes([9])*29
        self.assertTrue(self.consume(owner, 'partial-ready')['partial_ready'])
        owner.flows['alice'].crypto.partials = []
        self.assertFalse(self.consume(owner, 'partial-ready')['partial_ready'])
        owner.flows['alice'].crypto.partials = [dict(written=7)]
        write['values'][6] = 37
        self.assertFalse(self.consume(owner, 'partial-ready')['partial_ready'])

    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
    def test_local_open_prior_matches_native_sealed_event_before_release(self):
        records = self.recording(); owner = self.context(records)
        request = self.request('open-partial')
        event = records.rows('flow-state','agt1')[0]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            prior = dict(at=event['at'],endpoint=event['endpoint'].hex(),flow=event['flow'].hex(),
                         values=event['values'],kernel=request['kernel'],target=request['target'])
            peer.publish(root/'open-prior.json', prior)
            # Current host census may already see the same completed socket.
            request['kernel'] = dict(connections=[])
            request['target'] = dict(flows={'opening':dict(target_accepts=1)})
            with mock.patch.object(peer.observation,'Path',return_value=root), \
                 mock.patch.object(peer.observation,'fetch',return_value=records):
                # Root construction appends the nonce; use an actual matching directory.
                actual = root/NONCE
                actual.mkdir(exist_ok=False)
                try:
                    peer.publish(actual/'open-prior.json',prior)
                    self.assertTrue(peer.observation.product_receipt(owner,'open-partial',request)['open']['original_fd'])
                    prior['at'] += 1
                    peer.publish(actual/'open-prior.json',prior)
                    with self.assertRaises(peer.observation.CustodyError):
                        peer.observation.product_receipt(owner,'open-partial',request)
                finally:
                    (actual/'open-prior.json').unlink();actual.rmdir()

    def test_real_go_stream_drives_open_partial_and_collector_checkpoint(self):
        records=self.recording();owner=self.context(records)
        value=self.consume(owner,'open-partial')
        self.assertEqual((value['ss']['attempt_len'],value['ss']['n'],value['ss']['cipher_suffix_bytes']),(20,7,13))
        self.assertTrue(value['open']['original_fd'])
        with tempfile.TemporaryDirectory(prefix='s5-observation-collector-') as directory:
            root=Path(directory)
            args=SimpleNamespace(root=root,nonce=NONCE,profile='normal',arm='A')
            collector=fixture.Fixture(args,transport=mock.Mock())
            receipt=self.request('open-partial')
            collector.kernel=lambda: receipt['kernel']
            collector.sql=lambda _: [dict(port=14001,home_broker='brk1',epoch=0)]
            collector.read_events=lambda: []
            def remote(node,command,**fields):
                if command=='product-receipt':
                    return self.consume(owner,fields['label'],fields)
                return dict(flows={},identity=dict(pid=1,boot_id='boot',start_ticks=1))
            collector.remote_call=remote
            collector.snapshots['before']=dict(agent=receipt['kernel'])
            self.assertEqual(collector.checkpoint('open-partial'),dict(checkpoint='open-partial'))
            saved=json.loads((root/'open-partial.json').read_text())
            self.assertNotIn('data',saved['ss'])
            self.assertNotIn('missing_observations',saved)

    def test_partial_requires_actual_short_write_suffix_and_before_cut(self):
        import copy
        for field,value in ((0,7),(1,0),(1,20),(2,0)):
            records=self.recording();row=records.rows('ss-write','agt1')[0];row['values'][field]=value
            with self.subTest(field=field,value=value),self.assertRaises(peer.observation.CustodyError):
                records.partial()
        records=self.recording();records.rows('ss-write','agt1')[0]['data']=b''
        with self.assertRaises(peer.observation.CustodyError): records.partial()
        records=self.recording();records.rows('ss-write','agt1')[0]['actor']=bytes(16)
        with self.assertRaises(peer.observation.CustodyError): records.partial()
        records=self.recording();cut=copy.deepcopy(records.rows('ss-write','agt1')[0]);cut['kind']='final-cut';cut['at']-=1
        records.events.insert(0,cut)
        with self.assertRaises(peer.observation.CustodyError): records.partial()

    def test_custody_rejects_unsealed_truncated_poisoned_and_gapped_streams(self):
        records=self.recording();original=peer.observation.read_sealed(records.fds[0])
        for raw in (original[:70], original+b'foreign-tail', original[:64]+struct.pack('<Q',1)+original[72:]):
            fd=peer.sealed(raw)
            try:
                for retaining in (False, True):
                    with self.assertRaises(peer.observation.CustodyError): peer.observation.decode(fd,retain_events=retaining)
            finally: os.close(fd)
        fd=os.memfd_create('unsealed-control',os.MFD_ALLOW_SEALING)
        try:
            os.write(fd,original)
            for retaining in (False, True):
                with self.assertRaises(peer.observation.CustodyError): peer.observation.decode(fd,retain_events=retaining)
        finally: os.close(fd)
        # Intake validates the same complete immutable layout without keeping
        # the downstream judge's semantic event objects. Invalid event payloads
        # cannot become acceptable merely because custody stores only bytes.
        # origin: hot-update S5 plan §6.6 / §9.1 / DRILL-RUN 75.C
        header=b'TDRILL1\0'+bytes([1])*32+bytes([2])*16+struct.pack('<QQI',1,0,1)
        def frame(kind=b'ss-write',domain=b'ss',at=1,length=3,payload=b'xyz'):
            return header+bytes([len(kind)])+kind+bytes([len(domain)])+domain+bytes(92)+struct.pack('<17Q',at,*([0]*16))+struct.pack('<I',length)+payload
        for raw in (frame(kind=b'unknown'),frame(domain=b'unknown'),frame(at=0),frame(length=32769,payload=bytes(32769)),frame(length=4),frame()[:-1]):
            fd=peer.sealed(raw)
            try:
                for retaining in (False,True):
                    with self.assertRaises(peer.observation.CustodyError):peer.observation.decode(fd,retain_events=retaining)
            finally:os.close(fd)
        fd=peer.sealed(frame())
        try:
            original=peer.observation.decode(fd)
            stored=peer.observation.decode(fd,retain_events=False)
            self.assertEqual(stored[:3],original[:3])
            self.assertEqual(stored[3],[])
            self.assertEqual(original[3][0]['data'],b'xyz')
        finally:os.close(fd)
        duplicate=os.dup(records.fds[0])
        with self.assertRaises(peer.observation.CustodyError): records.add(duplicate,'agt1')
        with self.assertRaises(peer.observation.CustodyError): records.ready()

    def test_actual_subkey_nonce_ledger_rejects_aliases_and_missing_attempts(self):
        records=self.recording();self.assertEqual(len(records.nonce_ledger()),1)
        records.events.append(dict(records.rows('ss-seal','agt1')[0]))
        with self.assertRaises(peer.observation.CustodyError): records.nonce_ledger()
        records=self.recording();records.events=[e for e in records.events if e['kind']!='ss-seal']
        with self.assertRaises(peer.observation.CustodyError): records.nonce_ledger()

    def test_revoke_requires_original_binding_revision_and_zero_dups(self):
        records=self.recording('revoked');owner=self.context(records)
        request=self.request('revoked');request['kernel']['fds']=[]
        value=self.consume(owner,'revoked',request)
        self.assertEqual(value['victims'][0]['reservation'],0)
        self.assertEqual(value['fresh_ss_grant']['applied_revision'],4)
        request['kernel']['fds']=[dict(dev=1,ino=88,fd=17)]
        with self.assertRaises(peer.observation.CustodyError): self.consume(owner,'revoked',request)
        request['kernel']['fds']=[dict(dev=1,ino=99,fd=18)]
        with self.assertRaisesRegex(peer.observation.CustodyError,'victim_dup_not_zero'):
            self.consume(owner,'revoked',request)
        request['kernel']['fds']=[]
        records.events=[e for e in records.events if e['kind']!='ss-grant']
        with self.assertRaises(peer.observation.CustodyError): self.consume(owner,'revoked',request)

    # origin: hot-update S5 plan §9.1 / DRILL-RUN. The product owner can
    # be brk3; an instrumented but idle brk1 is not its FD/reaper evidence.
    def test_broker_receipt_follows_actual_allocation_owner(self):
        records = self.recording()
        for event in records.events:
            if event['source'] == 'brk1': event['source'] = 'brk3'
        records.identities['brk3'] = records.identities.pop('brk1')
        owner = self.context(records)
        request = self.request('allocation')
        request['broker_kernels'] = {'brk3': request.pop('broker_kernel')}
        value = self.consume(owner, 'allocation', request)
        self.assertGreater(value['broker_domains_by_node']['brk3']['flow']['reservation'], 0)
        self.assertGreater(value['reaper']['ticks'], 0)
        # The home is brk3 while a different elected voter owns the reaper.
        request['profile'] = 'capacity-32'
        for event in records.events:
            if event['kind'] == 'reaper': event['source'] = 'brk2'
        value = self.consume(owner, 'allocation', request)
        self.assertGreater(value['reaper']['ticks'], 0)
        request['broker_kernels'] = {'brk1': kernel()}
        with self.assertRaises(peer.observation.CustodyError):
            self.consume(owner, 'allocation', request)

    def test_budget_and_default_reaper_require_live_domain_receipts(self):
        records=self.recording();owner=self.context(records)
        value=self.consume(owner,'allocation')
        self.assertEqual(value['domains']['flow']['reservation'],8<<20)
        self.assertEqual(value['reaper']['default_dwell'],3)
        records.rows('domain-live','agt1')[0]['values'][2]=256<<20
        with self.assertRaises(peer.observation.CustodyError): self.consume(owner,'allocation')
        records.rows('domain-live','agt1')[0]['values'][2]=0
        records.rows('reaper','brk1')[0]['values'][2]=4
        with self.assertRaises(peer.observation.CustodyError): self.consume(owner,'allocation')
        records=self.recording();records.events=[e for e in records.events if e['kind']!='domain-live']
        with self.assertRaises(peer.observation.CustodyError): self.consume(self.context(records),'collect')

    def test_closed_actor_duplicates_remain_in_live_domain_census(self):
        records=self.recording('revoked')
        value=kernel();value['fds']=[dict(dev=1,ino=88,fd=11),dict(dev=1,ino=88,fd=12)]
        current=records.domains(value)
        self.assertEqual((current['ss']['fds'],current['ss']['dups']),(1,1))
        value['fds']=[]
        self.assertEqual(records.domains(value)['ss']['fds'],0)

    def test_progress_requires_current_original_offset_base_nonce_and_retirement(self):
        records=self.recording('progress');owner=self.context(records)
        first=lambda kind,key:{e[key]:e for e in reversed(records.rows(kind,'agt1'))}
        owner.observation_checkpoints['hop-1']=dict(flows=first('flow-state','flow'),actors=first('ss-state','actor'),
                                                   retired=first('flow-retirement','endpoint'))
        self.assertTrue(self.consume(owner,'active-progress')['retirement_advanced'])
        for field in ('flows','actors','retired'):
            original=owner.observation_checkpoints['hop-1'][field]
            kind,key={'flows':('flow-state','flow'),'actors':('ss-state','actor'),'retired':('flow-retirement','endpoint')}[field]
            owner.observation_checkpoints['hop-1'][field]={e[key]:e for e in records.rows(kind,'agt1')}
            with self.subTest(field=field),self.assertRaises(peer.observation.CustodyError): self.consume(owner,'active-progress')
            owner.observation_checkpoints['hop-1'][field]=original

    def test_legacy_requires_negotiated_empty_alpn_and_original_prefix(self):
        records=self.recording();self.assertTrue(records.legacy()['raw_prefix_observed'])
        records.rows('carrier-negotiated','agt1')[0]['data']=b'tether-flow/1'
        with self.assertRaises(peer.observation.CustodyError): records.legacy()
        records.rows('carrier-negotiated','agt1')[0]['data']=b''
        records.rows('carrier-prefix','agt1')[0]['data']=b'tether-flow/1'
        with self.assertRaises(peer.observation.CustodyError): records.legacy()
        records.rows('carrier-prefix','agt1')[0]['data']=bytes(12)
        records.rows('carrier-prefix','agt1')[0]['digest']=b'foreign'.ljust(32,b'!')
        with self.assertRaises(peer.observation.CustodyError): records.legacy()
        records.rows('carrier-prefix','agt1')[0]['data']=b''
        with self.assertRaises(peer.observation.CustodyError): records.legacy()

    def test_watchdog_requires_applied_failure_ready_and_automatic_current_cut(self):
        records=self.recording('watchdog');owner=self.context(records)
        self.assertTrue(self.consume(owner,'watchdog')['automatic'])
        for kind,index,value in (('commit-write-failed',1,0),('watchdog-current',1,119_000_000_000),
                                 ('watchdog-current',2,0),('final-cut',0,1)):
            row=records.rows(kind,'agt1')[-1];before=row['values'][index];row['values'][index]=value
            with self.subTest(kind=kind,index=index),self.assertRaises(peer.observation.CustodyError): self.consume(owner,'watchdog')
            row['values'][index]=before
        records.events=[e for e in records.events if e['kind']!='mark-ready']
        with self.assertRaises(peer.observation.CustodyError): self.consume(owner,'watchdog')

    def test_broker_replay_domain_is_a_separate_node_budget_and_fd_census(self):
        records=self.recording();owner=self.context(records);request=self.request('allocation')
        request['broker_kernel']=copy.deepcopy(request['kernel'])
        request['broker_kernel']['fds'].append(dict(dev=1,ino=99,fd=20))
        value=self.consume(owner,'allocation',request)
        self.assertEqual(value['broker_domains']['flow']['fds'],2)
        self.assertEqual(value['broker_domains']['flow']['reservation'],8<<20)
        records.rows('flow-state','brk1')[0]['values'][12]=257<<20
        with self.assertRaises(peer.observation.CustodyError):self.consume(owner,'allocation',request)
        records.rows('flow-state','brk1')[0]['values'][12]=8<<20
        del request['broker_kernel']
        with self.assertRaises(peer.observation.CustodyError):self.consume(owner,'allocation',request)

    def test_installer_preserves_product_uid_and_resolves_private_operator_group(self):
        spec=importlib.util.spec_from_file_location('observer_unit',LIB/'flow_observer_service.py')
        unit=importlib.util.module_from_spec(spec);spec.loader.exec_module(unit)
        with tempfile.TemporaryDirectory(prefix='s5-unit-controls-') as directory:
            root=Path(directory)
            def private_path(value):
                path=Path(value)
                return root/str(path).lstrip('/') if str(path).startswith(('/etc/systemd/','/run/tether-drill-')) else path
            with mock.patch.object(unit,'Path',side_effect=private_path),mock.patch.object(unit.pwd,'getpwnam',return_value=SimpleNamespace(pw_uid=os.getuid(),pw_gid=os.getgid())) as account,mock.patch.object(unit.subprocess,'check_output',return_value='[Service]\nUser=tether\nExecStart=/actual/artifact serve --config /public/config\n'),mock.patch.object(unit.subprocess,'run',return_value=SimpleNamespace(returncode=0)),mock.patch.object(sys,'argv',['unit','--node','brk1','--nonce',NONCE,'--script','/tmp/flow_observation.py']):
                self.assertEqual(unit.main(),0)
            account.assert_called_once_with('sim')
            custody=root/'run/tether-drill-custody'/NONCE
            self.assertEqual(custody.stat().st_mode&0o7777,0o2770)
            body=(root/'etc/systemd/system/tether-broker.service.d/zz-flow-observer.conf').read_text()
            self.assertIn('SupplementaryGroups=sim',body);self.assertNotIn('User=',body)
            self.assertIn('-- /actual/artifact serve --config /public/config',body)

    def test_common_judge_consumes_all_attempts_across_failed_writes_and_aliases(self):
        spec=importlib.util.spec_from_file_location('custody_oracle_controls',ROOT/'tests/hot-update-flow-probe-test.py')
        controls=importlib.util.module_from_spec(spec);spec.loader.exec_module(controls)
        sensor=controls.FlowProbeTests();sensor.setUp();self.addCleanup(sensor.doCleanups)
        records=self.recording();calls=records.nonce_ledger()
        sensor.document['product_attempts']={'fd':sensor.sealed(json.dumps(calls).encode())}
        self.assertEqual(sensor.invoke()[0],0)
        calls.append(dict(calls[0]))
        sensor.document['product_attempts']={'fd':sensor.sealed(json.dumps(calls).encode())}
        rc,value=sensor.invoke();self.assertEqual((rc,value['criterion']),(1,'nonce_reuse'))

    def test_payload_partial_counts_one_recorded_primitive_for_original_cipher(self):
        records=self.recording();calls=records.rows('ss-seal','agt1');write=records.rows('ss-write','agt1')[0]
        # Declared framing control: 36 bytes total, with 16 already consumed.
        write['values'][5:7]=[16,36]
        value=peer.observation.payload_partial(records,calls,write['actor'])
        self.assertEqual(value['seal_calls'],1)
        write['values'][6]+=1
        with self.assertRaises(peer.observation.CustodyError): peer.observation.payload_partial(records,calls,write['actor'])
        write['values'][6]-=1;write['nonce']=b'foreignnonce'
        with self.assertRaises(peer.observation.CustodyError): peer.observation.payload_partial(records,calls,write['actor'])

    def test_original_business_binding_uses_same_flow_and_actual_tls_session(self):
        import copy, hashlib
        records=self.recording();owner=self.context(records)
        identity={'tuple':['tcp','a'*64,'b'*64]}
        state=records.rows('flow-state','agt1')[0]
        socket_event=copy.deepcopy(state);socket_event.update(kind='flow-socket',digest=hashlib.sha256('\x00'.join(identity['tuple']).encode()).digest())
        records.events.append(socket_event)
        opening=copy.deepcopy(state);opening['values'][3]=1;opening['values'][5:8]=[0,0,0]
        records.events.insert(0,opening)
        row=dict(id='raw',protocol='raw',identities={'target':[identity]})
        value=peer.observation.bind_original(records,owner,row,'unchanged')
        self.assertEqual(value['flow_id'],state['flow'].hex())
        self.assertNotIn('digest',value)
        with self.assertRaises(peer.observation.CustodyError): peer.observation.bind_original(records,owner,row,'replaced')
        resumed=copy.deepcopy(records.rows('resume','agt1')[0]);resumed['digest']=b'new-session-identity'.ljust(32,b'!');resumed['values'][1]=2
        records.events.append(resumed)
        final=copy.deepcopy(state);final['values'][1]=2;records.events.append(final)
        self.assertNotEqual(peer.observation.bind_original(records,owner,row,'replaced')['before'],value['after'].rsplit(':',1)[0]+':2')
        final['values'][6]=999
        with self.assertRaises(peer.observation.CustodyError): peer.observation.bind_original(records,owner,row,'replaced')

    def test_legacy_negotiation_precedes_original_business(self):
        records=self.recording();owner=self.context(records)
        owner.flows={'raw':SimpleNamespace(start=time.monotonic()+1,sent=list(range(21)),received=list(range(21)))}
        event=copy.deepcopy(records.rows('carrier-negotiated','agt1')[0]);event.update(kind='legacy-binding');event['values'][:5]=[14001,1,7,1,77];records.events.append(event)
        self.assertTrue(self.consume(owner,'legacy-transport')['before_first_business'])
        records.rows('carrier-negotiated','agt1')[0]['at']=int((time.clock_gettime(time.CLOCK_BOOTTIME)+2)*1e9)
        with self.assertRaises(peer.observation.CustodyError):self.consume(owner,'legacy-transport')

    def test_legacy_original_socket_keeps_one_real_carrier_binding(self):
        import hashlib
        records=self.recording();owner=self.context(records)
        identity={'tuple':['tcp','a'*64,'b'*64]}
        event=copy.deepcopy(records.rows('carrier-negotiated','agt1')[0])
        event.update(kind='legacy-binding',data=hashlib.sha256('\x00'.join(identity['tuple']).encode()).digest())
        event['values'][:5]=[14001,1,7,1,77];records.events.append(event)
        row=dict(id='raw',protocol='raw',identities={'target':[identity]})
        value=peer.observation.bind_legacy_original(records,owner,row)
        self.assertEqual(value['before'],value['after']);self.assertEqual(value['legacy_stream'],7)
        changed=copy.deepcopy(event);changed['values'][4]=78;records.events.append(changed)
        with self.assertRaises(peer.observation.CustodyError): peer.observation.bind_legacy_original(records,owner,row)

    def test_keeper_requires_fresh_ack_and_preserves_sealed_archive_bytes(self):
        records=self.recording();raw=peer.observation.read_sealed(records.fds[0])
        archived=peer.observation.Records();self.addCleanup(archived.close)
        for sequence in range(1,301):
            batch=raw[:56]+struct.pack('<Q',sequence)+raw[64:]
            archived.add(peer.sealed(batch),'agt1')
        self.assertLess(len(archived.fds),50)
        for index in (0,127,255,299):
            fd,offset,length=archived.frames[index]
            observed=os.pread(fd,length,offset)
            self.assertEqual(observed,raw[:56]+struct.pack('<Q',index+1)+raw[64:])
            self.assertEqual(fcntl.fcntl(fd,fcntl.F_GET_SEALS)&peer.observation.SEALS,peer.observation.SEALS)
        with tempfile.TemporaryDirectory(prefix='s5-keeper-') as directory:
            root=Path(directory)
            producer,consumer=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
            config=peer.sealed(b'TDCFG1\0\0'+struct.pack('<Q',producer.fileno())+os.urandom(16))
            keeper=subprocess.Popen([sys.executable,'-I','-B',str(LIB/'flow_observation.py'),'keeper','--root',str(root),'--node','agt1','--fd',str(consumer.fileno())],pass_fds=(consumer.fileno(),),stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            consumer.close()
            env=dict(os.environ,TETHER_TEST_DRILL_OBSERVER='v1',TETHER_TEST_DRILL_CUSTODY_FD=str(config),TETHER_TEST_OBSERVER_RECORDING='keeper')
            product=subprocess.Popen([str(self.binary),'-test.run=^TestCustodyRecordedStream$'],pass_fds=(config,producer.fileno()),env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            producer.close();os.close(config)
            client=peer.observation.Records()
            try:
                deadline=time.monotonic()+5
                while not (root/'agt1.sock').exists():
                    self.assertLess(time.monotonic(),deadline);time.sleep(.01)
                self.assertEqual((root/'agt1.sock').stat().st_mode&0o777,0o660)
                peer.observation.fetch(client,root,'agt1');self.assertTrue(client.rows('ss-seal'))
                product.stdin.write(b'x');product.stdin.flush()
                self.assertEqual(product.stdout.readline().strip(),b'custody-poisoned')
                with self.assertRaises(peer.observation.CustodyError): peer.observation.fetch(client,root,'agt1')
                self.assertTrue(client.broken)
                product.communicate(input=b'y',timeout=5);self.assertEqual(product.returncode,0)
            finally:
                client.close()
                if product.poll() is None: product.kill();product.communicate(timeout=5)
                keeper.terminate();keeper.communicate(timeout=5)

    # Declared synthetic batches use the real keeper, sealed memfds and Unix
    # descriptor transfer. A fresh barrier on each tiny page consumed the
    # pending OPEN's two-second budget before the fault could be released.
    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
    def test_keeper_transfer_pages_preserve_all_frames_with_bounded_round_trips(self):
        records=self.recording();raw=peer.observation.read_sealed(records.fds[0])
        with tempfile.TemporaryDirectory(prefix='s5-keeper-pages-') as directory:
            root=Path(directory)
            producer,consumer=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
            keeper=subprocess.Popen([sys.executable,'-I','-B',str(LIB/'flow_observation.py'),'keeper','--root',str(root),'--node','agt1','--fd',str(consumer.fileno())],pass_fds=(consumer.fileno(),),stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            consumer.close()
            barriers=[];errors=[]
            def supply():
                try:
                    for sequence in range(1,301):
                        fd=peer.sealed(raw[:56]+struct.pack('<Q',sequence)+raw[64:])
                        try:
                            producer.sendmsg([b'TDC1'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,peer.array.array('i',[fd]))])
                        finally:
                            os.close(fd)
                    while True:
                        command=producer.recv(4)
                        if not command:
                            break
                        self.assertEqual(command,b'TDP1')
                        barriers.append(command)
                        producer.send(b'TDOK')
                except OSError:
                    pass
                except BaseException as error:
                    errors.append(error)
            thread=threading.Thread(target=supply);thread.start()
            client=peer.observation.Records()
            try:
                deadline=time.monotonic()+5
                while not (root/'agt1.sock').exists():
                    self.assertLess(time.monotonic(),deadline);time.sleep(.01)
                peer.observation.fetch(client,root,'agt1')
                self.assertEqual(len(client.frames),300)
                self.assertEqual(client.cursors['agt1'],300)
                self.assertEqual(len(barriers),3)
                for index,(fd,offset,length) in enumerate(client.frames):
                    self.assertEqual(os.pread(fd,length,offset),raw[:56]+struct.pack('<Q',index+1)+raw[64:])
                self.assertLess(len(client.fds),128)
                self.assertEqual(errors,[])
            finally:
                client.close()
                producer.shutdown(socket.SHUT_RDWR);producer.close()
                thread.join(timeout=5)
                keeper.terminate();keeper.communicate(timeout=5)
                self.assertFalse(thread.is_alive())

if __name__ == "__main__":
    unittest.main()
