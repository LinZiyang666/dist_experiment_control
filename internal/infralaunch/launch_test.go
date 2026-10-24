//go:build linux && amd64

package infralaunch

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
)

const launchHelperEnv = "_INFRA_LAUNCH_TEST_MODE"

var stockBuildOnce sync.Once
var stockTestBinary string
var stockBuildError error

// This is a hermetic stock-shaped process, not a claim about stock NATS. T63
// independently verifies both supplied stock releases with the same lock oracle.
const stockTestSource = `package main
import("crypto/sha256";"encoding/json";"io";"os";"runtime/debug";"strconv";"strings";"syscall")
type Config struct { Lock string; Device uint64; Inode uint64 }
type Receipt struct { PID int; Ticks uint64; LockRefs int; ForeignRefs int; Environment string; SHA [32]byte; Race bool }
func refs(c Config) (int,int) {
 entries,e:=os.ReadDir("/proc/self/fd");if e!=nil {panic(e)}; locks,foreign:=0,0
 for _,entry:=range entries {fd,e:=strconv.Atoi(entry.Name());if e!=nil||fd<3 {continue};var st syscall.Stat_t
  if syscall.Fstat(fd,&st)!=nil {continue};if uint64(st.Dev)==c.Device&&st.Ino==c.Inode {locks++}
  target,_:=os.Readlink("/proc/self/fd/"+entry.Name());if strings.Contains(target,"foreign-canary") {foreign++}
 };return locks,foreign
}
func main(){
 if len(os.Args)>1&&os.Args[1]=="probe" {var c Config;_ = json.Unmarshal([]byte(os.Args[2]),&c);n,_:=refs(c);if n!=0 {os.Exit(91)};os.Exit(0)}
 if len(os.Args)!=3||os.Args[1]!="--config" {os.Exit(92)}
 data,e:=os.ReadFile(os.Args[2]);if e!=nil {panic(e)};var c Config;if json.Unmarshal(data,&c)!=nil {os.Exit(93)}
 n,f:=refs(c);stat,e:=os.ReadFile("/proc/self/stat");if e!=nil {panic(e)};fields:=strings.Fields(string(stat)[strings.LastIndexByte(string(stat),')')+1:]);ticks,_:=strconv.ParseUint(fields[19],10,64)
 exe,e:=os.ReadFile("/proc/self/exe");if e!=nil {panic(e)}
 race:=false;if info,ok:=debug.ReadBuildInfo();ok {for _,setting:=range info.Settings {if setting.Key=="-race"&&setting.Value=="true" {race=true}}}
 receipt:=Receipt{os.Getpid(),ticks,n,f,os.Getenv("INFRA_SECRET_CANARY")+os.Getenv("_INFRA_LAUNCH_TEST_MODE")+os.Getenv("LD_PRELOAD"),sha256.Sum256(exe),race}
 if json.NewEncoder(os.Stdout).Encode(receipt)!=nil {os.Exit(94)}
 var input [1]byte;if _,e:=io.ReadFull(os.Stdin,input[:]);e!=nil {os.Exit(96)}
 if string(input[:])!="x" {os.Exit(97)}
}
`

func TestMain(m *testing.M) {
	rc := m.Run()
	if stockTestBinary != "" {
		_ = os.RemoveAll(filepath.Dir(stockTestBinary))
	}
	os.Exit(rc)
}

func stockBinary(t *testing.T) string {
	t.Helper()
	stockBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "infra-stock-shape-")
		if err != nil {
			stockBuildError = err
			return
		}
		stockTestBinary = filepath.Join(dir, "stock-shape")
		if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(stockTestSource), 0600); err != nil {
			stockBuildError = err
			return
		}
		cmd := exec.Command("go", "build", "-race", "-o", stockTestBinary, filepath.Join(dir, "main.go"))
		out, err := testharness.CombinedOutput(cmd)
		if err != nil {
			stockBuildError = fmt.Errorf("build stock-shaped witness: %w: %s", err, out)
			return
		}
		stockBuildError = os.Chmod(stockTestBinary, 0555)
	})
	if stockBuildError != nil {
		t.Fatal(stockBuildError)
	}
	return stockTestBinary
}

func launchDeadline(t *testing.T) infracontract.Deadline {
	t.Helper()
	d, err := controlbudget.After(20 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func hashFile(t *testing.T, f *os.File) infracontract.Digest {
	t.Helper()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, st.Size())); err != nil {
		t.Fatal(err)
	}
	var digest infracontract.Digest
	copy(digest[:], h.Sum(nil))
	return digest
}

func launchFixture(t *testing.T, binary string) (infracontract.HostRequest, Selection, *Lock) {
	t.Helper()
	t.Setenv("INVOCATION_ID", "launch-fixture")
	root := t.TempDir()
	for _, name := range []string{"store", "locks"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	open := func(path string) *os.File {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })
		return f
	}
	dir, locks := open(filepath.Join(root, "store")), open(filepath.Join(root, "locks"))
	guard, err := AcquireStore(dir, locks)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = guard.Close() })
	s := guard.Identity()
	config, err := json.Marshal(struct {
		Lock          string
		Device, Inode uint64
	}{filepath.Join(root, "locks", lockName(s)), s.LockDevice, s.LockInode})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "config.json")
	if err := os.WriteFile(path, config, 0600); err != nil {
		t.Fatal(err)
	}
	conf, exe := open(path), open(binary)
	current, err := CurrentProcess("launch-fixture")
	if err != nil {
		t.Fatal(err)
	}
	r := infracontract.HostRequest{Version: 1, Op: infracontract.LaunchClaim, Component: infracontract.NATS, Tx: infracontract.ID{1}, Step: 1, Domain: infracontract.ID{2}, Sequence: 1, PolicyRevision: 1, KeyID: "local-launch", HostBootID: current.BootID, Nonce: infracontract.ID{3}, Expected: current, Candidate: infracontract.Artifact{Kind: infracontract.NATSBinary, Version: "stock-shaped", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: infracontract.Digest{1}, BinarySHA256: hashFile(t, exe), CatalogSHA256: infracontract.Digest{2}}, Store: s, ExpectedJournal: infracontract.Digest{4}, Config: hashFile(t, conf), Payload: infracontract.Digest{5}, RemainingNS: uint64(20 * time.Second)}
	return r, Selection{Executable: exe, Config: conf, Directory: dir, Locks: locks, Guard: guard, Revision: 1, Journal: infracontract.Digest{6}}, guard
}

func helperEnvironment(mode, address string) []string {
	var env []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, launchHelperEnv+"=") && !strings.HasPrefix(entry, "_INFRA_LAUNCH_TEST_ADDRESS=") && !strings.HasPrefix(entry, "INFRA_SECRET_CANARY=") {
			env = append(env, entry)
		}
	}
	return append(env, launchHelperEnv+"="+mode, "_INFRA_LAUNCH_TEST_ADDRESS="+address, "INFRA_SECRET_CANARY=private-launch-canary", "GORACE=atexit_sleep_ms=0")
}

func helperCommand(t *testing.T, mode, address string, files []*os.File) (*exec.Cmd, func() error, *os.File, *os.File) {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(path, "-test.run=^TestNATSExecInheritsLifetimeLock$", "-test.count=1")
	cmd.Env = helperEnvironment(mode, address)
	cmd.ExtraFiles = files
	outputR, outputW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	inputR, inputW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stdin, cmd.Stderr = outputW, inputR, os.Stderr
	wait, err := testharness.StartProcess(cmd)
	_ = outputW.Close()
	_ = inputR.Close()
	if err != nil {
		_ = outputR.Close()
		_ = inputW.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = inputW.Close(); _ = wait(); _ = outputR.Close() })
	if err := outputR.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return cmd, wait, outputR, inputW
}

func exerciseLaunch(t *testing.T) {
	t.Helper()
	binary := stockBinary(t)
	r, selection, guard := launchFixture(t, binary)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(t.TempDir(), "launch.sock"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	if err := listener.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	foreign, err := os.OpenFile(filepath.Join(t.TempDir(), "foreign-canary"), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = foreign.Close() }()
	cmd, wait, output, input := helperCommand(t, "launch", listener.Addr().String(), []*os.File{foreign})
	decoder := json.NewDecoder(output)
	var before infracontract.Process
	if err := decoder.Decode(&before); err != nil {
		t.Fatal(err)
	}
	if before.PID != int32(cmd.Process.Pid) {
		t.Fatal("before receipt is not the supervised launcher")
	}
	r.Expected = before
	p, err := prepare(r, selection)
	if err != nil {
		t.Fatal(err)
	}
	p.deadline = launchDeadline(t)
	defer func() { _ = p.Close() }()
	// An independent worker owns the sender-side guard. Killing that actual
	// process must leave stock with the final reference, not release the lock.
	worker, workerWait, workerOutput, workerInput := helperCommand(t, "hold", "", []*os.File{guard.file})
	var held string
	if err := json.NewDecoder(workerOutput).Decode(&held); err != nil || held != "guard-held" {
		t.Fatalf("worker guard receipt: %q %v", held, err)
	}
	conn, err := listener.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := Send(conn, p, launchDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	var after struct {
		PID                   int
		Ticks                 uint64
		LockRefs, ForeignRefs int
		Environment           string
		SHA                   [32]byte
		Race                  bool
	}
	if err := decoder.Decode(&after); err != nil {
		t.Fatal(err)
	}
	if after.PID != int(before.PID) || after.Ticks != before.StartTicks || after.SHA != [32]byte(r.Candidate.BinarySHA256) {
		t.Fatalf("exec changed invocation or missed candidate: %+v", after)
	}
	if after.LockRefs != 1 || after.ForeignRefs != 0 || after.Environment != "" {
		t.Fatalf("exec allowlist violated: %+v", after)
	}
	if !after.Race {
		t.Fatal("inner stock witness was not race-instrumented")
	}
	actual, err := os.ReadFile(fmt.Sprintf("/proc/%d/exe", after.PID))
	if err != nil || sha256.Sum256(actual) != after.SHA {
		t.Fatalf("independent running image: %v", err)
	}
	if before.ExeSHA256 == infracontract.Digest(after.SHA) {
		t.Fatal("old launcher image survived exec")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	if err := worker.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = workerInput.Close()
	if err := workerWait(); err == nil {
		t.Fatal("worker SIGKILL was not observed")
	}
	if state, ok := worker.ProcessState.Sys().(syscall.WaitStatus); !ok || state.Signal() != syscall.SIGKILL {
		t.Fatal("sender worker did not exit through the injected SIGKILL")
	}
	assertStoreBusy(t, selection.Directory, selection.Locks)
	if _, err := input.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = input.Close()
	if err := wait(); err != nil {
		t.Fatalf("stock witness exit: %v", err)
	}
	lock, err := AcquireStore(selection.Directory, selection.Locks)
	if err != nil {
		t.Fatalf("actual server exit failed to release final reference: %v", err)
	}
	if lock.Identity() != r.Store {
		t.Fatal("permanent lock inode changed after server exit")
	}
	_ = lock.Close()
	t.Logf("stock-shaped exec pid=%d ticks=%d before_sha=%x after_sha=%x lock=%d:%d inherited_refs=%d sender_killed=true final_release=true", after.PID, after.Ticks, before.ExeSHA256, after.SHA, r.Store.LockDevice, r.Store.LockInode, after.LockRefs)
	exerciseTransport(t)
}

func assertStoreBusy(t *testing.T, directory, locks *os.File) {
	t.Helper()
	second, err := AcquireStore(directory, locks)
	if second != nil {
		_ = second.Close()
	}
	if err != ErrStoreBusy {
		t.Fatalf("second store owner admitted: %v", err)
	}
}

// origin: hot-update S9 plan §4.1 / T14
func TestNATSExecInheritsLifetimeLock(t *testing.T) {
	switch os.Getenv(launchHelperEnv) {
	case "hold":
		if err := json.NewEncoder(os.Stdout).Encode("guard-held"); err != nil {
			os.Exit(120)
		}
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	case "launch":
		self, err := CurrentProcess("launch-fixture")
		if err != nil {
			os.Exit(121)
		}
		if err := json.NewEncoder(os.Stdout).Encode(self); err != nil {
			os.Exit(122)
		}
		conn, err := net.Dial("unix", os.Getenv("_INFRA_LAUNCH_TEST_ADDRESS"))
		if err != nil {
			os.Exit(123)
		}
		p, err := receiveFrom(conn.(*net.UnixConn), infracontract.NATS, launchDeadline(t), nil, uint32(os.Geteuid()))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(124)
		}
		// Unrelated pre-exec work must not inherit the lock. Stock itself needs
		// the exceptional non-CLOEXEC reference only at the final exec boundary.
		config, err := io.ReadAll(io.NewSectionReader(p.config, 0, 1<<20))
		if err != nil {
			os.Exit(127)
		}
		probePath, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", p.executable.Fd()))
		if err != nil {
			os.Exit(129)
		}
		probe := exec.Command(probePath, "probe", string(config))
		probe.Env = []string{"PATH=/usr/bin:/bin", "GORACE=atexit_sleep_ms=0"}
		if err := testharness.RunProcess(probe); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(128)
		}
		if err := p.Exec(launchDeadline(t), nil); err != nil {
			fmt.Fprintln(os.Stderr, err)
			_ = p.Close()
			os.Exit(125)
		}
		os.Exit(126)
	default:
		exerciseLaunch(t)
	}
}

// origin: hot-update S9 plan §12 P06 / T14
func TestInfraLaunchLifecycle(t *testing.T) {
	_ = stockBinary(t) // Compiler supervision is warmed before the leak baseline.
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round%d", round), func(t *testing.T) { exerciseLaunch(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "infra launch", beforeG)
	testharness.AssertNoFDLeak(t, "infra launch", beforeFD, 0)
}
