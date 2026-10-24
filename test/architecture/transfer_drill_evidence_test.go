package architecture

// Standalone review regressions; copy into test/architecture or run this file
// directly. These import reader functions only, never execute a drill or tether.
// Local protocol peers are explicit test doubles, not deployment receipts.
import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

func reviewTransferPython(t *testing.T, script string) {
	t.Helper()
	root := os.Getenv("TRANSFER_REVIEW_ROOT")
	if root == "" {
		var err error
		root, err = os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		for {
			if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
				break
			}
			parent := filepath.Dir(root)
			if parent == root {
				t.Fatal("set TRANSFER_REVIEW_ROOT to repository root")
			}
			root = parent
		}
	}
	// ctx-root: bounded hermetic Python regression subprocess.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", "-I", "-B", "-c", reviewTransferPrelude+script, root)
	cmd.WaitDelay = time.Second
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	join, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := join(); err != nil {
			t.Error(err)
		}
	}()
	err = join()
	if err != nil {
		t.Fatalf("reader regression: %v\n%s", err, output.String())
	}
	t.Log(output.String())
}

const reviewTransferPrelude = `
import array, base64, copy, hashlib, importlib.util, json, os, socket, sys, tempfile, threading, time
from pathlib import Path
ROOT = Path(sys.argv[1])
sys.path.insert(0, str(ROOT / 'test/simcluster/drills/lib'))
import transfer_fixture as tf
import transfer_fixture_manifest as adapter
import transfer_keeper as tk
import transfer_mutations as tm
import transfer_observation as obs

def helper_module(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def reader_helpers():
    return helper_module('reader_controls', 'test/simcluster/tests/transfer-fixture-selftest.py')

def deployment(arm='B'):
    cohorts = []
    for i, label in enumerate(tm.COHORTS):
        small = label.startswith('small')
        cohorts.append(dict(label=label, sid='session'+str(i), bucket='bucket'+str(i),
            nid='node'+str(i), agent_node='agent'+str(i), binary_path='/isolated/bin'+str(i),
            home='/isolated/home'+str(i), source='/isolated/source'+str(i), destination='/isolated/target'+str(i),
            size=66549440 if small else 1 << 30, rate_limit_bps=262144 if small else 4194304,
            sha256='a'*64, verb='push' if label.endswith('push') else 'pull', force=small))
    topology = 'quorum' if arm == 'B' else 'single'
    result = dict(cohorts=cohorts, transport='caddy-wss',
        brokers=[dict(node='brk'+str(i), socket='/isolated/broker'+str(i)+'.sock')
                 for i in range(1, 4 if topology == 'quorum' else 2)],
        artifacts={key: dict(binary='/isolated/'+key, url='https://artifact.invalid/'+key,
            sha256=key.lower()*64, version='v1.0.0', transfer_version='transfer-v1') for key in ('A','B')})
    helper = helper_module('driver_controls', 'test/simcluster/tests/hot-update-transfer-drill-test.py')
    result['fixture'] = helper.fixture_deployment(result, arm, topology)
    return result
`

// origin: S7 internal review rv-drill F1
func TestTransferSnapshotProvidesSchedulerSample(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		reviewTransferPython(t, `
h = reader_helpers()
with h.live_fixture() as (fixture, _, _, _, _):
    fixture.snapshot()
    path = fixture.keeper.evidence / 'sample.json'
    assert path.exists(), 'sample.json missing: real fixture wrote only snapshot.json'
`)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), beforeG)
	// Two descriptors allow the Go runtime's first netpoll initialization.
	testharness.AssertNoFDLeak(t, t.Name(), beforeFD, 2)
}

// origin: S7 internal review rv-drill F2
func TestTransferProjectionRejectsMissingCohortLegAndBrokerHops(t *testing.T) {
	reviewTransferPython(t, `
value = deployment()
# All driver steps remain intact. Remove only the observer's required cells.
value['fixture']['cuts'] = value['fixture']['cuts'][:1]
with tempfile.TemporaryDirectory() as directory:
    src, dst = Path(directory)/'driver.json', Path(directory)/'fixture.json'
    src.write_text(json.dumps(tm.make_manifest(value, 'B')))
    rejected = False
    try:
        adapter.write_manifest(src, dst)
    except (ValueError, KeyError, TypeError):
        rejected = True
    assert rejected, 'accepted one observer cut for N3; all other cohorts/legs/hops omitted'
`)
}

// origin: S7 internal review rv-drill F2
func TestTransferCoverageRequiresInstallBetweenDurableReceipts(t *testing.T) {
	reviewTransferPython(t, `
fixture = object.__new__(tf.Fixture)
cut = dict(transfer_id='t', producer='agent', leg='leg2', generation=2,
           role='receiver', cut_id='required-cut', max_after_first_ns=190_000_000_000)
fixture.manifest = dict(cuts=[cut], transfers=[dict(transfer_id='t', size=100)])
def row(kind, generation, when, **extra):
    return dict(transfer_id='t', producer='agent', leg='leg2', kind=kind,
                generation=generation, boottime_ns=when, cut_id='', **extra)
events = [row('synced', 1, 1, durable_offset=10),
          row('synced', 2, 2, durable_offset=100),
          dict(row('install', 2, 3), cut_id='required-cut')]
missing = fixture.coverage(events)
assert missing, 'coverage accepted completed data before install, with no post-install progress'
`)
}

// origin: S7 internal review rv-drill F3
func TestTransferKeeperConsumesProducerOFDProtocol(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		reviewTransferPython(t, `
with tempfile.TemporaryDirectory() as directory:
    keeper = tk.Keeper(dict(processes=[dict(label='agent')], transfers=[]), directory)
    producer, config = keeper.lease('agent')
    os.close(config)
    # Exact P28 framing: TDO1 plus sealed metadata and the duplicate OFD.
    metadata = obs.sealed(json.dumps(dict(schema=1, pid=os.getpid(), process_start=1,
        boot_id='boot', resource=dict(resource_id='source', kind='xfer-source-v1', name='source'),
        witness_digest='a'*64, owned_fds=1, peak_fds=2)).encode())
    original = os.open(Path(directory)/'source', os.O_CREAT|os.O_RDWR, 0o600)
    try:
        keeper.start()
        producer.sendmsg([b'TDO1'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [metadata, original]))])
        deadline = time.monotonic()+2
        while not keeper.setup_failures and not keeper.witnesses and time.monotonic()<deadline:
            time.sleep(.01)
        assert 'custody_reply' not in keeper.setup_failures, 'real producer TDO1 rejected as custody_reply'
    finally:
        keeper.close()
        producer.close()
        os.close(metadata)
        os.close(original)
`)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), beforeG)
	// Two descriptors allow the Go runtime's first netpoll initialization.
	testharness.AssertNoFDLeak(t, t.Name(), beforeFD, 2)
}

// origin: S7 internal review rv-drill F4
func TestTransferDrainRejectsReplayedAcknowledgement(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		reviewTransferPython(t, `
with tempfile.TemporaryDirectory() as directory:
    keeper = tk.Keeper(dict(processes=[dict(label='agent')], transfers=[]), directory)
    producer, config = keeper.lease('agent')
    os.close(config)
    producer.settimeout(2)
    packets, errors = [], []
    def replay_peer():
        try:
            for _ in range(2):
                packets.append(producer.recv(64))
                producer.send(b'TDOK') # identical replay, no fresh watermark/nonce
        except Exception as error:
            errors.append(str(error))
    thread = threading.Thread(target=replay_peer)
    try:
        keeper.start()
        thread.start()
        first, second = keeper.drain(timeout=2), keeper.drain(timeout=2)
        assert False, 'accepted replayed TDOK twice: packets=%r cursors=%r' % (packets, [first['cursor'], second['cursor']])
    except obs.SetupError:
        pass
    finally:
        thread.join(3)
        keeper.close()
        producer.close()
        assert not thread.is_alive()
`)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), beforeG)
	// Two descriptors allow the Go runtime's first netpoll initialization.
	testharness.AssertNoFDLeak(t, t.Name(), beforeFD, 2)
}

// origin: S7 internal review rv-drill F5
func TestTransferRollbackScheduleDoesNotReturnToFinishedLeg(t *testing.T) {
	reviewTransferPython(t, `
for arm in ('R1', 'R2'):
    steps = tm.requirements(arm, 'single')
    legs = [s['leg'] for s in steps]
    assert legs == sorted(legs), '%s reuses original four cohorts in impossible leg order %r' % (arm, legs)
`)
}

// origin: S7 internal review rv-drill F6
func TestTransferControlBudgetIncludesRetriesAndTerminalOutcome(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		reviewTransferPython(t, `
h = reader_helpers()
with h.live_fixture() as (fixture, producer, _, _, _):
    fixture.manifest['control'] = dict(logical_ids=['slow-logical-call'])
    producer.emit([
        h.event('attempt', 5, attempt=dict(logical_id='slow-logical-call', ordinal=1, phase='send',
            started_mono_ns=1, finished_mono_ns=4_000_000_001, sent=True, **{'class':'retry'})),
        h.event('attempt', 6, attempt=dict(logical_id='slow-logical-call', ordinal=2, phase='send',
            started_mono_ns=4_000_000_001, finished_mono_ns=8_000_000_001, sent=True, **{'class':'retry'})),
    ])
    deadline = time.monotonic()+3
    while len(fixture.keeper.records.events)<6 and time.monotonic()<deadline:
        time.sleep(.01)
    assert len(fixture.keeper.records.events)==6
    result = fixture.judge()
    assert result['native_rc'] != 0, 'GREEN for 8-second logical call with only failed retries and no terminal success'
`)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), beforeG)
	// Two descriptors allow the Go runtime's first netpoll initialization.
	testharness.AssertNoFDLeak(t, t.Name(), beforeFD, 2)
}

// origin: S7 internal review rv-drill F7
func TestTransferCollectorAllowsDeclaredRecoverableServerOutage(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		reviewTransferPython(t, `
# The background collector's error policy is tested in isolation; no real
# server restart, deployment command or process is launched.
with tempfile.TemporaryDirectory() as directory:
    manifest = dict(processes=[], transfers=[], arm='C3', cuts=[], nats={}, manifest_sha256='a'*64)
    fixture = tf.Fixture(manifest, directory)
    recovered = threading.Event()
    calls = [0]
    def scan():
        calls[0] += 1
        if calls[0] == 1:
            raise ConnectionRefusedError('declared same-store restart')
        recovered.set()
        return {}
    fixture.scan = scan
    try:
        fixture.start()
        assert recovered.wait(2), 'collector did not retry'
        assert not fixture.keeper.setup_failures, 'recovered expected C3 outage permanently poisons reader: '+repr(fixture.keeper.setup_failures)
    finally:
        fixture.close()
`)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), beforeG)
	// Two descriptors allow the Go runtime's first netpoll initialization.
	testharness.AssertNoFDLeak(t, t.Name(), beforeFD, 2)
}

// origin: S7 internal review rv-drill F3.
func TestTransferOFDGoSendReachesPythonCollector(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("sealed custody requires Linux")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferOFDGoSend(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), beforeG)
	// Two descriptors allow the Go runtime's first netpoll initialization.
	testharness.AssertNoFDLeak(t, t.Name(), beforeFD, 2)
}

func exerciseTransferOFDGoSend(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("sealed custody requires Linux")
	}
	file, err := os.CreateTemp(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if _, err := file.WriteString("original inode"); err != nil {
		t.Fatal(err)
	}
	witness, err := drillobserve.CaptureTransferOFD(file, struct{ ResourceID, Name, Kind string }{"source", "source", "xfer-source-v1"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := witness.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET, 0)
	if err != nil {
		t.Fatal(err)
	}
	unix.CloseOnExec(pair[0])
	unix.CloseOnExec(pair[1])
	defer func() {
		if err := unix.Close(pair[0]); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	consumer := os.NewFile(uintptr(pair[1]), "python-custody")
	defer func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err := witness.Send(pair[0]); err != nil {
		t.Fatal(err)
	}
	// ctx-root: bounded cross-language observer process, reaped by the shared supervisor.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", "-I", "-B", "-c", `
import hashlib, os, socket, sys, tempfile, time
from pathlib import Path
sys.path.insert(0, sys.argv[1] + '/test/simcluster/drills/lib')
import transfer_keeper as k
pid = int(sys.argv[2])
with tempfile.TemporaryDirectory() as directory:
    identity = k.process_identity(pid)
    keeper = k.Keeper(dict(processes=[dict(label='agent', artifact_shas=[identity['running_sha']])],
        transfers=[dict(transfer_id='original', target=sys.argv[3], size=14, sha256='a'*64,
                        required_resources={'source':['cut']})]), directory)
    keeper.channels['agent'] = socket.socket(fileno=3)
    try:
        keeper.start()
        deadline = time.monotonic()+3
        while not keeper.witnesses and not keeper.setup_failures and time.monotonic()<deadline:
            time.sleep(.01)
        assert not keeper.setup_failures, keeper.setup_failures
        assert list(keeper.witnesses) == ['source'], keeper.witnesses
        original = keeper.witnesses['source']
        assert k.compare_ofd(pid, int(sys.argv[4]), original['fd'])
        reopened = os.open(sys.argv[3], os.O_RDWR)
        try:
            os.lseek(reopened, 14, os.SEEK_SET)
            assert not k.compare_ofd(os.getpid(), reopened, original['fd']), 'same-inode reopen accepted'
        finally:
            os.close(reopened)
    finally:
        keeper.close()
`, repoRoot(t), strconv.Itoa(os.Getpid()), file.Name(), strconv.Itoa(int(file.Fd())))
	cmd.ExtraFiles = []*os.File{consumer}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	join, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := join(); err != nil {
			t.Error(err)
		}
	}()
	if err := join(); err != nil {
		t.Fatalf("Go Send/Python collector: %v\n%s", err, output.String())
	}
}

// origin: S7 internal review rv-drill F4.
func TestTransferDrainRequiresFreshNonceWatermarkAndNoPoison(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		reviewTransferPython(t, `
h = reader_helpers()
for defect in ('replay', 'nonce', 'watermark', 'unchanged', 'poison'):
    with tempfile.TemporaryDirectory() as directory:
        keeper = tk.Keeper(dict(processes=[dict(label='agent')], transfers=[]), directory)
        producer, config = keeper.lease('agent')
        os.close(config)
        identity = keeper.identities['agent']
        incarnation = os.urandom(16)
        producer.settimeout(1)
        import struct
        def send_batch(sequence):
            fd = obs.sealed(h.envelope(identity, incarnation, sequence, []))
            try:
                producer.sendmsg([b'TDC1'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [fd]))])
            finally:
                os.close(fd)
        send_batch(1)
        def peer():
            request = producer.recv(64)
            send_batch(2)
            ack = b'TDA2' + request[4:] + struct.pack('<QQ', 2, 0)
            producer.send(ack)
            request = producer.recv(64)
            if defect != 'unchanged': send_batch(3)
            if defect == 'replay': producer.send(ack)
            else: producer.send(b'TDA2' + (bytes(16) if defect == 'nonce' else request[4:]) +
                                struct.pack('<QQ', 2 if defect in ('watermark','unchanged') else 3, int(defect == 'poison')))
        thread = threading.Thread(target=peer)
        try:
            keeper.start()
            deadline = time.monotonic()+1
            while not keeper.records.sequences and time.monotonic()<deadline: time.sleep(.01)
            thread.start()
            keeper.drain(1)
            try: keeper.drain(1)
            except obs.SetupError: pass
            else: raise AssertionError('accepted '+defect)
        finally:
            thread.join(2)
            keeper.close()
            producer.close()
            assert not thread.is_alive()
`)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), beforeG)
	// Two descriptors allow the Go runtime's first netpoll initialization.
	testharness.AssertNoFDLeak(t, t.Name(), beforeFD, 2)
}

// origin: S7 internal review rv-drill F6.
func TestTransferControlClockRejectsMissingAndUnfinishedSlots(t *testing.T) {
	reviewTransferPython(t, `
from transfer_oracle import judge_control
h = reader_helpers()
start = 1_000_000_000
control = dict(baseline_ns=start, drain_ns=start+200_000_000, logical_ids=['one','two'])
def attempt(logical, ordinal, start, finish, cls='ok'):
    return h.event('attempt', ordinal, native_outcome=0, attempt=dict(logical_id=logical, ordinal=ordinal,
        phase='send', started_mono_ns=start, finished_mono_ns=finish, sent=True, **{'class':cls}, backoff_ns=0))
rows = [attempt('one', 1, start, start+20_000_000), attempt('two', 1, start+100_000_000,start+120_000_000)]
effects = {key:dict(native_rc=0,effect_count=1,observed=True,finished_ns=start+120_000_000) for key in ('one','two')}
judge_control(control, rows, effects)
# S6 records backoff as its own elapsed phase; DNS/TLS may be nested inside
# a connect phase. Neither shape spends the elapsed duration a second time.
backoff=[attempt('one',1,start,start+20_000_000,'retry'),
         attempt('one',2,start+20_000_000,start+40_000_000),
         attempt('one',3,start+40_000_000,start+60_000_000),rows[1]]
backoff[1]['attempt'].update(phase='backoff',sent=False,backoff_ns=20_000_000)
judge_control(control,backoff,effects)
nested=[attempt('one',1,start,start+60_000_000),
        attempt('one',2,start+10_000_000,start+20_000_000),
        attempt('one',3,start+60_000_000,start+70_000_000),rows[1]]
nested[0]['attempt']['phase']='connect'
nested[1]['attempt']['phase']='dns'
judge_control(control,nested,effects)
for defect in ('total-budget','only-retries','empty-slots','missing-times','missing-slot','missing-effect','native-nonzero','missing-backoff','backoff-duration'):
    c, r, e = copy.deepcopy(control), copy.deepcopy(rows), copy.deepcopy(effects)
    if defect == 'total-budget':
        r = [attempt('one',1,start,start+4_000_000_000,'retry'),
             attempt('one',2,start+4_000_000_000,start+8_000_000_000),r[1]]
    elif defect == 'only-retries': r[0]['attempt']['class']='retry'
    elif defect == 'empty-slots': c['logical_ids']=[]; r=[]
    elif defect == 'missing-times': del r[0]['attempt']['finished_mono_ns']
    elif defect == 'missing-slot': r.pop()
    elif defect == 'native-nonzero': r[0]['native_outcome']=75
    elif defect == 'missing-backoff': del r[0]['attempt']['backoff_ns']
    elif defect == 'backoff-duration': r=copy.deepcopy(backoff); r[1]['attempt']['backoff_ns']=40_000_000
    else: del e['one']
    try:
        for row in r: obs.validate_event(row)
        judge_control(c,r,e)
    except (obs.SetupError,KeyError): pass
    else: raise AssertionError('accepted '+defect)
`)
}

// origin: S7 internal review rv-drill F2.
func TestTransferCutBindsIdentityGenerationAndStrictProgress(t *testing.T) {
	reviewTransferPython(t, `
from transfer_oracle import cut_coverage, responsibility
cut=dict(transfer_id='t',producer='agent',leg='leg2',generation=2,role='receiver',cut_id='cut',max_after_first_ns=100)
manifest=dict(cuts=[cut],transfers=[dict(transfer_id='t',size=100)])
def row(kind,generation,when,offset):
    return dict(kind=kind,generation=generation,boottime_ns=when,durable_offset=offset,
        last_consumed_stream_seq=offset,transfer_id='t',producer='agent',leg='leg2',phase='receiving',
        pid=1,process_start=2,boot_id='boot',cut_id='cut' if kind=='install' else '')
rows=[row('synced',1,1,10),row('install',2,3,0),row('synced',2,4,20)]
assert not cut_coverage(manifest,rows)
for defect in ('pid','start','boot','generation','cut','post-time','no-progress','finished','missing-ack'):
    broken=copy.deepcopy(rows)
    if defect=='pid': broken[-1]['pid']=9
    elif defect=='start': broken[-1]['process_start']=9
    elif defect=='boot': broken[-1]['boot_id']='different'
    elif defect=='generation': broken[1]['generation']=3
    elif defect=='cut': broken[1]['cut_id']='foreign'
    elif defect=='post-time': broken[-1]['boottime_ns']=2
    elif defect=='no-progress': broken[-1]['durable_offset']=10
    elif defect=='finished': broken[0]['durable_offset']=100; broken[-1]['durable_offset']=101
    else: broken[0]['last_consumed_stream_seq']=0
    assert cut_coverage(manifest,broken),'accepted '+defect
assert responsibility(dict(kind='control',phase='prepared'),'prepared')
assert not responsibility(dict(kind='append',actual_ack_seq=1),'prepared')
assert responsibility(dict(kind='finish',phase='terminal_pending',canonical_digest='a'*64),'courier')
assert responsibility(dict(kind='reserve',revision=1,original_deadline={'boot_id':'boot','deadline':9}),'tracker')
`)
}

// origin: S7 internal review rv-drill F3.
func TestTransferOFDErrorBranchesCloseEveryReceivedDescriptor(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		reviewTransferPython(t, `
for round in range(5):
    for defect in ('unsealed','malformed','identity','foreign'):
        with tempfile.TemporaryDirectory() as directory:
            before=len(os.listdir('/proc/self/fd'))
            identity=tk.process_identity(os.getpid())
            manifest=dict(processes=[dict(label='agent',artifact_shas=[identity['running_sha']])],transfers=[])
            keeper=tk.Keeper(manifest,directory)
            producer,config=keeper.lease('agent')
            os.close(config)
            row=dict(schema=1,pid=identity['pid'],process_start=identity['process_start'],boot_id=identity['boot_id'],
                     resource=dict(resource_id='source',kind='xfer-source-v1',name='source'),
                     witness_digest='a'*64,owned_fds=1,peak_fds=2)
            if defect=='identity': row['process_start']+=1
            raw=b'{' if defect=='malformed' else json.dumps(row).encode()
            if defect=='unsealed':
                metadata=os.memfd_create('bad-transfer-ofd',os.MFD_CLOEXEC|os.MFD_ALLOW_SEALING)
                os.write(metadata,raw)
            else: metadata=obs.sealed(raw)
            source=os.open(Path(directory)/'source',os.O_CREAT|os.O_RDWR,0o600)
            try:
                keeper.start()
                producer.sendmsg([b'TDO1'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[metadata,source]))])
                deadline=time.monotonic()+2
                while not keeper.setup_failures and time.monotonic()<deadline: time.sleep(.01)
                assert keeper.setup_failures and not keeper.witnesses,defect
            finally:
                keeper.close(); producer.close(); os.close(metadata); os.close(source)
            assert len(os.listdir('/proc/self/fd'))==before,'descriptor leaked: '+defect
`)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), beforeG)
	// Two descriptors allow the Go runtime's first netpoll initialization.
	testharness.AssertNoFDLeak(t, t.Name(), beforeFD, 2)
}

// origin: S7 internal review rv-drill F1. The bootstrap must activate the real
// Go observer, not just pass a descriptor to a permissive Python test peer.
func TestTransferLaunchBootstrapActivatesRealGoObserver(t *testing.T) {
	if os.Getenv("S7_DRL_LAUNCH_PEER") == "1" {
		if drillobserve.Hook == nil {
			t.Fatal("launch bootstrap did not activate the real Go observer")
		}
		return
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("S7_DRL_LAUNCH_BINARY", binary)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		reviewTransferPython(t, `
import subprocess,sys,hashlib
h=reader_helpers()
with tempfile.TemporaryDirectory() as directory:
    root=Path(directory)
    wire=h.ProtocolServer(b'payload')
    evidence=root/'evidence'
    target=root/'target'; target.write_bytes(b'payload')
    binary=os.environ['S7_DRL_LAUNCH_BINARY']
    manifest=dict(schema=1,mode='hermetic',quiet_seconds=2,cuts=[],nats=dict(url=wire.url),
        processes=[dict(label='ctl1',artifact_shas=[hashlib.sha256(Path(binary).read_bytes()).hexdigest()])],
        transfers=[dict(transfer_id='original',entry_nonce='nonce',object_version='NUID',object_name='object',
            bucket='bucket',session_id='session',cohort='hermetic',target=str(target),size=7,
            sha256=hashlib.sha256(b'payload').hexdigest(),object_stream='OBJ_bucket',
            history_stream='history',history_subject='history.original',required_resources={})])
    path=root/'manifest.json'; path.write_text(json.dumps(manifest))
    command=[sys.executable,'-I','-B',str(Path(tf.__file__)),'start','--manifest',str(path),'--evidence',str(evidence)]
    with (root/'fixture.out').open('w') as log:
        child=subprocess.Popen(command,stdout=log,stderr=log)
    try:
        deadline=time.monotonic()+5
        while not (evidence/'control.sock').exists() and time.monotonic()<deadline:
            assert child.poll() is None,(root/'fixture.out').read_text()
            time.sleep(.01)
        assert (evidence/'control.sock').exists()
        code='import sys,json; sys.path.insert(0,sys.argv[1]); from transfer_fixture_manifest import exec_owner; exec_owner(*json.loads(sys.argv[2]))'
        args=[str(evidence/'control.sock'),hashlib.sha256(path.read_bytes()).hexdigest(),'ctl1',
              [binary,'-test.run=^TestTransferLaunchBootstrapActivatesRealGoObserver$','-test.count=1'],str(root)]
        result=subprocess.run([sys.executable,'-I','-B','-c',code,str(Path(tf.__file__).parent),json.dumps(args)],
                              env=dict(os.environ,S7_DRL_LAUNCH_PEER='1'),capture_output=True,text=True,timeout=10)
        assert result.returncode==0,result.stdout+result.stderr
    finally:
        child.terminate()
        try: child.wait(timeout=5)
        except subprocess.TimeoutExpired: child.kill(); child.wait()
        wire.close()
`)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), beforeG)
	// Two descriptors allow the Go runtime's first netpoll initialization.
	testharness.AssertNoFDLeak(t, t.Name(), beforeFD, 2)
}
