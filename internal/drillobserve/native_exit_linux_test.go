//go:build linux

package drillobserve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// origin: S7 internal review rv-drill F1 / observer exit-release handoff
func TestNativeExitKeepsObserverUntilFinalSealedACK(t *testing.T) {
	binaryPath := filepath.Join(t.TempDir(), "observer-cli")
	buildNativeTestBinary(t, binaryPath, "../../cmd/tether")
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseNativeExit(t, binaryPath)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseNativeExit(t *testing.T, binaryPath string) {
	t.Helper()
	for _, args := range [][]string{{"version"}, {"--definitely-invalid-observer-option"}} {
		func() {
			config, socket, receiver := nativeExitPair(t)
			defer func() {
				if err := config.Close(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			defer func() {
				if err := socket.Close(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			defer func() {
				if err := unix.Close(receiver); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			var rawConfig [32]byte
			if _, err := config.ReadAt(rawConfig[:], 0); err != nil {
				t.Fatal(err)
			}
			reader := TransferCustodyReader{Identity: sha256.Sum256(rawConfig[:])}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binaryPath, args...)
			cmd.Env = append(StripEnvironment(os.Environ()), "HOME="+t.TempDir(), EnableEnv+"=v1", CustodyEnv+"=3")
			cmd.ExtraFiles = []*os.File{config, socket}
			join, err := testharness.StartProcess(cmd)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = join() }()
			if err := unix.SetsockoptTimeval(receiver, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Sec: 3}); err != nil {
				t.Fatal(err)
			}
			read := func() ([]Event, []byte) {
				t.Helper()
				data, oob := make([]byte, 64), make([]byte, unix.CmsgSpace(4))
				n, cn, flags, _, err := unix.Recvmsg(receiver, data, oob, 0)
				for err == unix.EINTR {
					n, cn, flags, _, err = unix.Recvmsg(receiver, data, oob, 0)
				}
				if err != nil || n == 0 || flags != 0 {
					t.Fatalf("observer exited or failed before final sealed ACK: n=%d err=%v", n, err)
				}
				if string(data[:n]) != "TDC1" {
					return nil, data[:n]
				}
				controls, err := unix.ParseSocketControlMessage(oob[:cn])
				if err != nil || len(controls) != 1 {
					t.Fatal("sealed receipt missing")
				}
				fds, err := unix.ParseUnixRights(&controls[0])
				if err != nil || len(fds) != 1 {
					t.Fatal("sealed receipt FD missing")
				}
				defer func() {
					if err := unix.Close(fds[0]); err != nil {
						t.Errorf("cleanup: %v", err)
					}
				}()
				seals, err := unix.FcntlInt(uintptr(fds[0]), unix.F_GET_SEALS, 0)
				if err != nil || seals&custodySeals != custodySeals {
					t.Fatal("native receipt unsealed")
				}
				var stat unix.Stat_t
				if unix.Fstat(fds[0], &stat) != nil {
					t.Fatal("receipt stat")
				}
				raw := make([]byte, int(stat.Size))
				if _, err := unix.Pread(fds[0], raw, 0); err != nil {
					t.Fatal(err)
				}
				events, err := reader.Accept(raw)
				if err != nil {
					t.Fatal(err)
				}
				return events, nil
			}
			var result struct {
				PID          int    `json:"pid"`
				NativeRC     int    `json:"native_rc"`
				ProcessStart uint64 `json:"process_start"`
				BootID       string `json:"boot_id"`
			}
			found := false
			for !found {
				events, packet := read()
				if packet != nil {
					t.Fatal("unexpected native packet")
				}
				for _, event := range events {
					if event.Kind == "native-exit" && event.Domain == "exec" {
						if json.Unmarshal(event.Data, &result) != nil {
							t.Fatal("native result malformed")
						}
						found = true
					}
				}
			}
			if result.PID != cmd.Process.Pid || result.ProcessStart == 0 || result.BootID == "" || (len(args) == 1 && args[0] == "version") != (result.NativeRC == 0) {
				t.Fatalf("actual native result lost: %+v", result)
			}
			var nonce [16]byte
			for barrier := byte(1); barrier <= 2; barrier++ {
				nonce[0] = barrier
				request, _ := TransferDrainRequest(nonce)
				if err := unix.Sendmsg(receiver, request, nil, nil, 0); err != nil {
					t.Fatal(err)
				}
				previous := reader.Sequence()
				for {
					_, ack := read()
					if ack == nil {
						continue
					}
					if reader.Sequence() <= previous || reader.Finish(nonce, ack) != nil {
						t.Fatal("final sealed ACK missing or stale")
					}
					break
				}
				if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
					t.Fatal("original CLI exited before release")
				}
			}
			release := append([]byte("TDR2"), nonce[:]...)
			release = binary.LittleEndian.AppendUint64(release, reader.Sequence())
			if err := unix.Sendmsg(receiver, release, nil, nil, 0); err != nil {
				t.Fatal(err)
			}
			err = join()
			if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != result.NativeRC {
				t.Fatalf("supervised native exit changed: state=%v err=%v sealed=%d", cmd.ProcessState, err, result.NativeRC)
			}
			// Descriptors are closed in this exercise, before leak checks.
			// The deferred checks above run on every exit, including failed ACK assertions.
		}()
	}
}

func buildNativeTestBinary(t *testing.T, path, pkg string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", path, pkg)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	join, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := join(); err != nil {
		t.Fatalf("build: %v: %s", err, output.String())
	}
}

func nativeExitPair(t *testing.T, legacy ...bool) (*os.File, *os.File, int) {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := unix.MemfdCreate("native-exit-config", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		_ = unix.Close(pair[0])
		_ = unix.Close(pair[1])
		t.Fatal(err)
	}
	config, socket := os.NewFile(uintptr(fd), "native-exit-config"), os.NewFile(uintptr(pair[0]), "native-exit-channel")
	magic := "TDCFG2\x00\x00"
	if len(legacy) != 0 && legacy[0] {
		magic = "TDCFG1\x00\x00"
	}
	data := binary.LittleEndian.AppendUint64([]byte(magic), 4)
	data = append(data, bytes.Repeat([]byte{7}, 16)...)
	if _, err := config.Write(data); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, custodySeals); err != nil {
		t.Fatal(err)
	}
	return config, socket, pair[1]
}

// origin: S7 internal review rv-drill F1 / finalization across native CLI and Python keeper
func TestNativeExitFixtureReleasesAndVerifiesSupervisedResult(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	binaryPath, supervisor := filepath.Join(dir, "observer-cli"), filepath.Join(dir, "supervise")
	buildNativeTestBinary(t, binaryPath, "../../cmd/tether")
	buildNativeTestBinary(t, supervisor, "../../test/fullstackharness/cmd/supervise")
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseNativeExitFixture(t, root, binaryPath, supervisor)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseNativeExitFixture(t *testing.T, root, binaryPath, supervisor string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", "-I", "-B", "-c", nativeExitFixtureScript, root, binaryPath, supervisor)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	join, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := join(); err != nil {
		t.Fatalf("fixture finalization: %v: %s", err, output.String())
	}
}

const nativeExitFixtureScript = `
import array, hashlib, json, os, socket, sys, tempfile, threading, time
from pathlib import Path
sys.path.insert(0, str(Path(sys.argv[1])/'test/simcluster/drills/lib'))
from transfer_fixture import Fixture
from transfer_fixture_manifest import LaunchOwner
from transfer_keeper import Keeper, process_identity
from transfer_observation import require, SetupError
binary, supervisor = sys.argv[2:]
os.environ['TRANSFER_PROCESS_SUPERVISOR'] = supervisor
artifact = hashlib.sha256(Path(binary).read_bytes()).hexdigest()
for argv in ([binary,'version'],[binary,'--definitely-invalid-observer-option']):
 with tempfile.TemporaryDirectory() as directory:
  root = Path(directory)
  transfer = dict(transfer_id='original',ctl_producer='ctl',target=str(root/'target'),old=None,size=1,sha256=hashlib.sha256(b'x').hexdigest())
  (root/'target').write_bytes(b'x')
  spec = dict(label='ctl',role='ctl',artifact_shas=[artifact],launch=dict(home=str(root/'private-home'),argv=argv))
  fixture = Fixture.__new__(Fixture)
  fixture.manifest = dict(processes=[spec],transfers=[transfer],manifest_sha256='a'*64,mode='live')
  fixture.keeper = Keeper(fixture.manifest,root)
  fixture.launcher, fixture.drains = LaunchOwner(fixture), []
  with socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET) as service:
   service.bind(str(root/'control.sock')); service.listen(1); service.settimeout(5)
   def lease():
    channel,_ = service.accept()
    with channel:
     request=json.loads(channel.recv(65536))
     require(request['command']=='lease' and request['label']=='ctl','lease request')
     producer,config=fixture.keeper.lease('ctl')
     try: channel.sendmsg([b'{"native_rc":0}'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[producer.fileno(),config]))])
     finally: producer.close(); os.close(config)
   thread=threading.Thread(target=lease); thread.start()
   try:
    fixture.keeper.start(); fixture.launcher.launch(spec)
    thread.join(5); assert not thread.is_alive()
    deadline=time.monotonic()+5
    while True:
     with fixture.keeper.lock:
      frames=list(fixture.keeper.records.frames)
      if frames:
       raw=os.pread(frames[0],os.fstat(frames[0]).st_size,0)
       offset=76
       kind_size=raw[offset]; offset+=1+kind_size
       domain_size=raw[offset]; offset+=1+domain_size
       value=json.loads(raw[offset+232:])
       original=process_identity(value['pid'])
       assert original['running_sha']==artifact
       fixture.keeper.processes['ctl']=original
       break
     assert time.monotonic()<deadline
     time.sleep(.01)
    result=fixture.native_results()['ctl']
    assert (result['native_rc']==0)==(argv[-1]=='version')
    assert fixture.launcher.children[0][1].poll() is None
    for _ in range(3): fixture.drains.append(fixture.keeper.drain())
    assert fixture.launcher.children[0][1].poll() is None
    fixture.release_originals()
    assert fixture.launcher.children[0][1].poll()==result['native_rc']
    assert not fixture.keeper.setup_failures,fixture.keeper.setup_failures
    receipt=json.loads((root/'native-release.json').read_bytes())
    assert receipt['results']['ctl']==result
   finally:
    fixture.keeper.close(); fixture.launcher.close(); thread.join(5)
    for channel in getattr(fixture,'released_channels',[]): channel.close()
`

// origin: S7 internal review rv-drill F1 / release is bound to the final sealed ACK
func TestNativeExitReleaseRequiresFreshWatermark(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseNativeReleaseGuard(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseNativeReleaseGuard(t *testing.T) {
	t.Helper()
	for _, defect := range []string{"nonce", "watermark", "zero", "uncompleted", "poison"} {
		func() {
			pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
			if err != nil {
				t.Fatal(err)
			}
			c := &custody{channel: pair[0], identity: sha256.Sum256([]byte("release-guard")), incarnation: [16]byte{1}, queue: make(chan pending, queueDepth), exitReady: true, exitDone: make(chan struct{})}
			if defect == "uncompleted" {
				c.exitReady = false
			}
			if defect == "poison" {
				poisonCustody(c, 2)
			}
			go c.writeLoop()
			defer func() {
				_ = unix.Shutdown(pair[1], unix.SHUT_RDWR)
				select {
				case <-c.exitDone:
				case <-time.After(time.Second):
					t.Error("release writer did not join")
				}
				_ = unix.Close(pair[0])
				_ = unix.Close(pair[1])
			}()
			nonce := [16]byte{1}
			request, _ := TransferDrainRequest(nonce)
			if unix.Sendmsg(pair[1], request, nil, nil, 0) != nil {
				t.Fatal("drain send")
			}
			var sequence uint64
			for {
				packet, fds := readNativeGuardPacket(t, pair[1])
				for _, fd := range fds {
					var header [72]byte
					_, _ = unix.Pread(fd, header[:], 0)
					sequence = binary.LittleEndian.Uint64(header[56:64])
					_ = unix.Close(fd)
				}
				if string(packet[:4]) == "TDA2" {
					if defect != "poison" && ValidateTransferDrainACK(packet, nonce, sequence) != nil {
						t.Fatal("release guard baseline ACK")
					}
					break
				}
			}
			release := append([]byte("TDR2"), nonce[:]...)
			if defect == "nonce" {
				release[4]++
			}
			if defect == "watermark" {
				sequence++
			}
			if defect == "zero" {
				sequence = 0
			}
			release = binary.LittleEndian.AppendUint64(release, sequence)
			if unix.Sendmsg(pair[1], release, nil, nil, 0) != nil {
				t.Fatal("release send")
			}
			// The producer must emit poisoned sealed evidence, never a clean release.
			for {
				_, fds := readNativeGuardPacket(t, pair[1])
				if len(fds) == 0 {
					t.Fatal("release fabricated an unsealed ACK")
				}
				poisoned := false
				for _, fd := range fds {
					var header [72]byte
					_, _ = unix.Pread(fd, header[:], 0)
					poisoned = poisoned || binary.LittleEndian.Uint64(header[64:72]) != 0
					_ = unix.Close(fd)
				}
				if poisoned {
					break
				}
			}
			if !c.broken.Load() {
				t.Fatalf("%s release escaped custody poison", defect)
			}
		}()
	}
}

func readNativeGuardPacket(t *testing.T, fd int) ([]byte, []int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		n, err := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 50)
		if err == unix.EINTR || n == 0 {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		packet, fds, err := receiveTransferPacket(fd)
		if err != nil {
			t.Fatal("custody ended without poisoned sealed evidence")
		}
		return packet, fds
	}
	t.Fatal("native release evidence missing")
	return nil, nil
}

// origin: S7 internal review rv-drill F1 / a receipt file cannot replace custody
func TestNativeExitFixtureRejectsMissingLaunchOwner(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseNativeMissingLaunchOwner(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseNativeMissingLaunchOwner(t *testing.T) {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", "-I", "-B", "-c", `
import sys,types
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'test/simcluster/drills/lib'))
from transfer_fixture import Fixture
from transfer_observation import SetupError
instance=Fixture.__new__(Fixture)
instance.manifest={'mode':'live'}
instance.launcher=None
def unexpected_wait(): raise AssertionError('missing launch owner began a product byte wait')
instance.snapshot=unexpected_wait
failures=[]
instance.keeper=types.SimpleNamespace(poison=failures.append)
try: instance.await_originals()
except SetupError as error: assert str(error)=='native_ctl_receipt_missing'
else: raise AssertionError('missing launch owner consumed a transfer budget')
assert failures==['native_ctl_receipt_missing']
`, root)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	join, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := join(); err != nil {
		t.Fatalf("native owner guard: %v: %s", err, output.String())
	}
}

// origin: S7 internal review rv-drill F1 / existing keepers never need TDR2
func TestNativeExitLegacyCustodyRetainsImmediateExit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observer-cli")
	buildNativeTestBinary(t, path, "../../cmd/tether")
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLegacyNativeExit(t, path)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseLegacyNativeExit(t *testing.T, path string) {
	t.Helper()
	config, socket, receiver := nativeExitPair(t, true)
	defer func() {
		if err := config.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	defer func() {
		if err := socket.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	defer func() {
		if err := unix.Close(receiver); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, "version")
	cmd.Env = append(StripEnvironment(os.Environ()), "HOME="+t.TempDir(), EnableEnv+"=v1", CustodyEnv+"=3")
	cmd.ExtraFiles = []*os.File{config, socket}
	join, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := join(); err != nil || cmd.ProcessState.ExitCode() != 0 {
		t.Fatalf("legacy custody postponed native exit for an unsupported release: %v", err)
	}
}
