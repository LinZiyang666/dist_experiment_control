//go:build linux && hotupdate_integration

// Assembly guards use supervised fixtures and real Unix custody sockets.
// Native DTO controls do not certify live product cuts; P25 remains required.
package hotupdate_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	"slices"
)

// Encode the existing evidence format, including its contract-specific types.
func encodeAssemblyEvidence(t *testing.T, v reflect.Value) json.RawMessage {
	t.Helper()
	schemas := map[reflect.Type]string{
		reflect.TypeOf(c.Digest{}): "digest", reflect.TypeOf(c.Key{}): "key",
		reflect.TypeOf(c.Process{}): "process",
	}
	if schema := schemas[v.Type()]; schema != "" {
		b, err := c.EncodeJSON(schema, v.Interface())
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	value := v.Interface()
	switch v.Kind() {
	case reflect.Struct:
		m := map[string]json.RawMessage{}
		for i := 0; i < v.NumField(); i++ {
			f := v.Type().Field(i)
			name := strings.Split(f.Tag.Get("json"), ",")[0]
			if name == "" {
				name = f.Name
			}
			m[name] = encodeAssemblyEvidence(t, v.Field(i))
		}
		value = m
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			rows := make([]json.RawMessage, v.Len())
			for i := range rows {
				rows[i] = encodeAssemblyEvidence(t, v.Index(i))
			}
			value = rows
		}
	}
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Pure oracle data must never sign a live product round.
// The separate lease helper cannot authenticate these synthetic product rows.
// origin: S10 internal review rv3-assembly F2
func TestOwnershipProductRejectsSyntheticRounds(t *testing.T) {
	root := t.TempDir()
	old := *transactionEvidenceDir
	*transactionEvidenceDir = root
	t.Cleanup(func() { *transactionEvidenceDir = old })
	for round := 0; round < 5; round++ {
		e := fullstackOwnershipControl()
		e.Round, e.RunID = round, fmt.Sprintf("s10/p24/round-%d/ownership", round)
		for i := range e.LockProbes {
			e.LockProbes[i].Contender = e.Initial[0].Stamp.Process
			e.LockProbes[i].Contender.PID = 300
			e.LockProbes[i].Contender.InstanceID = "invented-contender"
		}
		path := filepath.Join(root, e.RunID+".json")
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encodeAssemblyEvidence(t, reflect.ValueOf(e)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for round := 0; round < 5; round++ {
		name := fmt.Sprintf("s10/p24/round-%d/ownership", round)
		if _, err := fullstackOwnershipFromRun(t.Context(), name, round, nil, c.Evidence{}, c.Evidence{}); err == nil {
			t.Fatal("synthetic ownership archive signed a product round without a live fixture")
		}
	}
}

func exerciseAssemblyPython(t *testing.T, program string) {
	t.Helper()
	lib, err := filepath.Abs("../simcluster/drills/lib")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/usr/bin/python3", "-I", "-B", "-c", program, lib, t.TempDir())
	output, err := fullstackharness.CombinedOutput(cmd)
	if err != nil {
		t.Fatalf("reproducer: %v\n%s", err, output)
	}
	t.Log(string(output))
}

// origin: S10 internal review rv3-assembly F1
func TestAssemblyRejectsUnboundExecutables(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUnboundAssembly(t)
	}
	testharness.AssertNoGoroutineLeak(t, "assembly rejection lifecycle", g)
	testharness.AssertNoFDLeak(t, "assembly rejection lifecycle", fd, 0)
}

func exerciseUnboundAssembly(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	supervisor := filepath.Join(t.TempDir(), "supervise")
	build := exec.CommandContext(ctx, "go", "build", "-o", supervisor, "./test/fullstackharness/cmd/supervise")
	build.Dir = "../.."
	if output, err := fullstackharness.CombinedOutput(build); err != nil {
		t.Fatalf("supervisor build: %v\n%s", err, output)
	}
	t.Setenv("REVIEW_SUPERVISOR", supervisor)
	exerciseAssemblyPython(t, `
import json, os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import fullstack_fixture as f
base = Path(sys.argv[2]); root = base / 'evidence'
profile = base / 'profile.json'
profile.write_text(json.dumps(dict(tier='deploy', id=f.PREFIX+'A', topology='single', evidence_root=str(root))))
judge = base / 'unbound-judge'
judge.write_text('#!/usr/bin/python3\nprint(\'{"rc":0,"verdict":"GREEN"}\')\n')
judge.chmod(0o700)
# All three processes exit normally; no tether, Docker, SSH or drill is run.
try:
    f.run_host('A', root, profile, '/usr/bin/true', os.environ['REVIEW_SUPERVISOR'], str(judge))
except (ValueError, f.Incomplete):
    pass
else:
    raise AssertionError('unbound host and judge signed GREEN without evidence')
assert not (root / 'evidence.json').exists()
children = []
for arm in f.ARMS:
    row = dict(arm=arm, instance='invented-'+arm, native_rc=0, verdict='GREEN')
    row['raw'] = f.archive(base, arm+'.json', row.copy())
    children.append(row)
try:
    rc = f.join_children(f.load_manifest(), children)
except (ValueError, f.Incomplete):
    pass
else:
    assert rc != 0, 'self-reported child receipts signed GREEN'
print('unbound executables and self-reported children rejected')
`)
}

// origin: S10 internal review rv3-assembly F3
func TestAssemblyDoesNotReleasePendingDependency(t *testing.T) {
	exerciseAssemblyPython(t, `
import copy, sys
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0, sys.argv[1])
import fullstack_fixture as f
import fullstack_fixture_test as fixture_test
root = Path(sys.argv[2]); p = fixture_test.profile(root)
calls = []
def pending(method, payload):
    calls.append(method)
    return f.canonical(dict(stamp={'tx_id':'still-pending'}, historical={}, current={}, phase='staged',
        outcome='pending', code='', detail='', deferred=[], control={}, resources={}, released={}, raw={}))
window = fixture_test.window()
raw = f.archive(root, 'unrelated.json', {'work_performed': False})
ports = SimpleNamespace(call_driver=pending, before_step=lambda *a: copy.deepcopy(window))
ports.after_step = lambda step, obligations, *a: dict(obligations={name: raw for name in obligations}, window=copy.deepcopy(window))
assembly = f.Assembly(p, {}, ports)
assembly.phase = 'running'
step = next(row['step'] for row in assembly.steps if row['step']['parent_id']=='R1' and row['step']['target']['role']=='nats')
assembly.completed.update(step['depends_on'])
try:
    assembly.apply(step['id'])
except (ValueError, f.Incomplete):
    pass
else:
    raise AssertionError('pending infra step released its dependency')
assert step['id'] not in assembly.completed
print('pending infra step retained dependency')
`)
}

// origin: S10 internal review rv3-assembly F7
func TestCustodyRejectionClosesNonSocketFD(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCustodyRejection(t)
	}
	testharness.AssertNoGoroutineLeak(t, "assembly rejection lifecycle", g)
	testharness.AssertNoFDLeak(t, "assembly rejection lifecycle", fd, 0)
}

func exerciseCustodyRejection(t *testing.T) {
	exerciseAssemblyPython(t, `
import array, os, socket, sys, threading
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import flow_observer_service as f
root = Path(sys.argv[2]); server = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
server.bind(str(root/'a0.sock')); server.listen(1)
r, w = os.pipe()
def send():
    with server.accept()[0] as peer:
        peer.recv(32)
        peer.sendmsg([b'fullstack-custody-v1'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [r]))])
thread = threading.Thread(target=send)
def links():
    result = {}
    for name in os.listdir('/proc/self/fd'):
        try: result[int(name)] = os.readlink('/proc/self/fd/'+name)
        except FileNotFoundError: pass
    return result
before = links()
thread.start()
try:
    f.open_custody(root, 'a0')
    raise AssertionError('non-socket accepted')
except OSError:
    pass
thread.join(); server.close(); os.close(r); os.close(w)
# Compare stable kernel links rather than the directory iterator's transient FD.
leaked = [fd for fd, link in links().items() if fd > 2 and fd not in before and link.startswith('pipe:')]
for fd in leaked: os.close(fd)
assert not leaked, leaked
print('rejected non-socket FD returned to baseline')
import unittest
import flow_observer_service_test as regression
result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(regression.ObserverCustodyTest))
assert result.wasSuccessful(), 'custody rejection matrix failed' 
`)
}

// origin: S10 internal review rv3-assembly F5
func TestObserverTimeoutStillJoinsEveryChild(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseObserverTimeout(t)
	}
	testharness.AssertNoGoroutineLeak(t, "assembly rejection lifecycle", g)
	testharness.AssertNoFDLeak(t, "assembly rejection lifecycle", fd, 0)
}

func exerciseObserverTimeout(t *testing.T) {
	exerciseAssemblyPython(t, `
import os, signal, subprocess, sys
from pathlib import Path
from unittest.mock import Mock, patch
sys.path.insert(0, sys.argv[1])
import flow_observer_service as f
root = Path(sys.argv[2]); (root/'a0.sock').touch()
keeper, product = Mock(), Mock()
keeper.poll.return_value = None
keeper.wait.return_value = 0
product.poll.return_value = None
product.wait.side_effect = subprocess.TimeoutExpired('unresponsive-product-supervisor', 15)
saved = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
def child(supervisor, root, label, argv, stdin, children):
    value = keeper if label.endswith('.supervisor') else product
    children.append(value)
    if value is product:
        # Enter normal requested shutdown with both original children live.
        os.kill(os.getpid(), signal.SIGTERM)
    return value
try:
    with patch.object(f, 'supervised_child', side_effect=child):
        try:
            f.launch(root, 'a0', Path(f.__file__), Path('/usr/bin/true'), [sys.executable])
        except subprocess.CalledProcessError as error:
            assert error.returncode != 0
            assert len(error.output) >= 2 and error.output[-1] == 0, error.output
        else:
            raise AssertionError('timeout was not propagated')
    assert product.terminate.call_count >= 1 and product.wait.call_count >= 1
    assert keeper.terminate.call_count == 1 and keeper.wait.call_count == 1
    assert all(signal.getsignal(sig) == handler for sig, handler in saved.items())
    print('every registered child joined and handlers restored after timeout')
finally:
    for sig, handler in saved.items(): signal.signal(sig, handler)
`)
}

// origin: S10 internal review rv3-assembly F4
func TestFlowAcceptsHostScopedUIDs(t *testing.T) {
	exerciseAssemblyPython(t, `
import sys
sys.path.insert(0, sys.argv[1])
import fullstack_provision as p
import fullstack_provision_test as fixture
import fullstack_flow as f
manifest = fixture.manifest()
rows, online = fixture.observations()
for i, row in enumerate(rows):
    # Two fresh hosts independently allocate their first two service accounts.
    uid = 1400 + i % 2
    row['key']['uid'] = row['binary']['uid'] = row['config']['uid'] = uid
ready = p.ready_roster(manifest, rows, online)
bindings = {}
for i, row in enumerate(ready['runtimes']):
    bindings[row['runtime']] = dict(key=row['key'], route=dict(realm='lab', nats='nats://brk1:4222',
        wss='wss://brk1:443/nats', client_host='ctl1', target_root='/tmp/target-'+str(i),
        client_root='/tmp/client-'+str(i), host='brk1', port=20000+i, ssh_user='sim-a'+str(i)))
sources = f.original_sources(f.peer_module(), manifest['nonce'], 1, bindings)
assert len(sources) == 4
bindings['a1']['key']['uid'] = bindings['a0']['key']['uid']
try:
    f.original_sources(f.peer_module(), manifest['nonce'], 1, bindings)
except ValueError:
    pass
else:
    raise AssertionError('same host shared UID accepted')
print('host-scoped UIDs accepted; same-host shared UID rejected')
`)
}

// origin: S10 internal review rv3-assembly F5
func TestObserverStoppedSupervisorStillReaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	supervisor := filepath.Join(t.TempDir(), "supervise")
	build := exec.CommandContext(ctx, "go", "build", "-o", supervisor, "./test/fullstackharness/cmd/supervise")
	build.Dir = "../.."
	if output, err := fullstackharness.CombinedOutput(build); err != nil {
		t.Fatalf("supervisor build: %v\n%s", err, output)
	}
	t.Setenv("REVIEW_SUPERVISOR", supervisor)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseStoppedObserver(t)
	}
	testharness.AssertNoGoroutineLeak(t, "stopped observer supervisors", g)
	testharness.AssertNoFDLeak(t, "stopped observer supervisors", fd, 0)
}

func exerciseStoppedObserver(t *testing.T) {
	exerciseAssemblyPython(t, `
import json, os, signal, subprocess, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import flow_observer_service as f
root = Path(sys.argv[2]); children = []
try:
    for label in ('keeper', 'product'):
        f.supervised_child(Path(os.environ['REVIEW_SUPERVISOR']), root, label, ['/bin/sleep', '60'], subprocess.DEVNULL, children)
    targets = [json.loads((root/(label+'.json')).read_text())['target'] for label in ('keeper', 'product')]
    os.kill(children[-1].pid, signal.SIGSTOP)
    try:
        f.join_supervised_children(children)
    except subprocess.CalledProcessError as error:
        assert error.returncode == 125, error.returncode
    else:
        raise AssertionError('timeout cleanup signed success')
    assert all(child.returncode is not None for child in children), 'original supervisor waiter omitted'
    for target in targets:
        path = Path('/proc')/str(target['pid'])/'stat'
        if path.exists():
            ticks = int(path.read_text().rsplit(')', 1)[1].split()[19])
            assert ticks != target['start_ticks'], 'original target survived observer Join'
    print('stopped real supervisor resumed; both original waiters and target trees reaped')
finally:
    # Mutation failures still return every actual process to its original waiter.
    for child in children:
        if child.poll() is None:
            child.send_signal(signal.SIGCONT); child.terminate()
        child.wait(timeout=15)
`)
}

// Controls imported from the pending S8 supplier predate the authoritative
// custody vocabulary and separate scope/expected digests. Seal only this local
// oracle's declared scope after adding its retained classes. Product evidence
// never passes through this function and remains unchanged at the predicate.
// origin: S10 internal review rv3-assembly F3 / native completion control
func fullstackSealNativeControl(observation *upgradetx.Observation) {
	a := &observation.Current
	a.Scope.Expected = slices.Clone(a.Scope.Expected)
	for i := range a.Scope.Expected {
		a.Scope.Expected[i].Custody = "owned"
	}
	scope, err := upgradetx.ScopeDigest(a.Scope)
	if err != nil {
		panic(err)
	}
	expected, err := upgradetx.ExpectedDigest(a.Scope)
	if err != nil {
		panic(err)
	}
	for _, receipt := range []*upgradetx.Receipt{&a.Control, &a.Resources, &a.Released} {
		receipt.Stamp.ScopeDigest = scope
		receipt.Census.Stamp, receipt.Census.ExpectedDigest = receipt.Stamp, expected
		receipt.Census.Resources = slices.Clone(receipt.Census.Resources)
		for i := range receipt.Census.Resources {
			receipt.Census.Resources[i].Custody = "owned"
		}
	}
	observation.Census.Stamp.ScopeDigest, observation.Census.ExpectedDigest = scope, expected
	observation.Census.Resources = slices.Clone(observation.Census.Resources)
	for i := range observation.Census.Resources {
		observation.Census.Resources[i].Custody = "owned"
	}
}
