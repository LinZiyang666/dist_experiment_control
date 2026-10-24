//go:build linux && amd64

package infraroot

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infralaunch"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// This compiled, race-enabled witness exercises exec/OFD mechanics. It makes
// no claim about external stock releases, systemd, nft or multiple kernel UIDs.
const journalStockSource = `package main
import("crypto/sha256";"encoding/json";"io";"os";"runtime/debug";"strconv";"strings";"syscall")
func main(){
 if len(os.Args)!=3||os.Args[1]!="--config" {os.Exit(91)}
 var c struct{Device,Inode uint64};b,e:=os.ReadFile(os.Args[2]);if e!=nil||json.Unmarshal(b,&c)!=nil {os.Exit(92)}
 entries,e:=os.ReadDir("/proc/self/fd");if e!=nil{panic(e)};refs:=0
 for _,entry:=range entries{fd,e:=strconv.Atoi(entry.Name());if e!=nil{continue};var s syscall.Stat_t;if syscall.Fstat(fd,&s)==nil&&uint64(s.Dev)==c.Device&&s.Ino==c.Inode{refs++}}
 stat,e:=os.ReadFile("/proc/self/stat");if e!=nil{panic(e)};fields:=strings.Fields(string(stat)[strings.LastIndexByte(string(stat),')')+1:]);ticks,_:=strconv.ParseUint(fields[19],10,64)
 exe,e:=os.ReadFile("/proc/self/exe");if e!=nil{panic(e)};race:=false;if info,ok:=debug.ReadBuildInfo();ok{for _,s:=range info.Settings{if s.Key=="-race"&&s.Value=="true"{race=true}}}
 if json.NewEncoder(os.Stdout).Encode(struct{PID int;Ticks uint64;SHA [32]byte;Refs int;Race bool}{os.Getpid(),ticks,sha256.Sum256(exe),refs,race})!=nil{os.Exit(93)}
 var input [1]byte;if _,e:=io.ReadFull(os.Stdin,input[:]);e!=nil||input[0]!='x'{os.Exit(94)}
}`

// origin: S9 internal review rv-infra F01 / F03 / F04
func TestInstalledRootJournalsAuthenticatedSamePIDExec(t *testing.T) {
	if os.Getenv("_INFRA_ROOT_EXEC_CHILD") == "1" {
		rootExecChild(t)
		return
	}
	dir := t.TempDir()
	source := filepath.Join(dir, "main.go")
	binary := filepath.Join(dir, "stock-shaped")
	if err := os.WriteFile(source, []byte(journalStockSource), 0600); err != nil {
		t.Fatal(err)
	}
	if out, err := testharness.CombinedOutput(exec.Command("go", "build", "-race", "-o", binary, source)); err != nil {
		t.Fatalf("compile exec witness: %v %s", err, out)
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseRootJournalExec(t, binary) })
	}
	testharness.AssertNoGoroutineLeak(t, "installed root journal exec workers", beforeG)
	testharness.AssertNoFDLeak(t, "installed root journal exec custody", beforeFD, 0)
}

func rootExecChild(t *testing.T) {
	fail := func(err error) { fmt.Fprintln(os.Stderr, err); t.Fatal(err) }
	credentials, endpoint, metadata := os.NewFile(3, "credentials"), os.NewFile(4, "endpoint"), os.NewFile(5, "launch identity")
	defer closeTestResource(t, credentials)
	defer closeTestResource(t, endpoint)
	defer closeTestResource(t, metadata)
	var identity infrapriv.LaunchIdentity
	if err := json.NewDecoder(metadata).Decode(&identity); err != nil {
		fail(err)
	}
	var gate [1]byte
	if _, err := io.ReadFull(os.Stdin, gate[:]); err != nil || gate[0] != 'g' {
		fail(fmt.Errorf("parent gate: %v", err))
	}
	key, err := infrapriv.ReadCredentialStore(credentials, infrapriv.LocalLaunch, uint32(os.Geteuid()), uint32(os.Getegid()))
	if err != nil {
		fail(err)
	}
	defer closeTestResource(t, key)
	client, err := infrapriv.NewPinnedClient(endpoint, uint32(os.Geteuid()), uint32(os.Getegid()))
	if err != nil {
		fail(err)
	}
	d, err := controlbudget.After(12 * time.Second)
	if err != nil {
		fail(err)
	}
	prepared, finish, err := infralaunch.AcquireLaunch(client, identity, key, c.NATS, d, nil)
	if err != nil {
		fail(err)
	}
	defer finish()
	defer closeTestResource(t, prepared)
	if err := prepared.Exec(d, nil); err != nil {
		fail(err)
	}
	t.Fatal("exec returned without replacement")
}

func exerciseRootJournalExec(t *testing.T, binary string) {
	t.Helper()
	i, manager, _ := installedFixture(t)
	defer closeTestResource(t, i.files)
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	i.boot = strings.TrimSpace(string(boot))
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	launcherBytes, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	if err := i.files.Replace("/usr/local/libexec/tether-infra-launch", launcherBytes, 0755, i.gid); err != nil {
		t.Fatal(err)
	}
	stock, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	candidate := c.Artifact{Kind: c.NATSBinary, Version: "stock-shaped", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: digest("fixture archive"), BinarySHA256: sha256.Sum256(stock), CatalogSHA256: digest("fixture catalog")}
	if err := os.MkdirAll(i.files.Path("/var/lib/tether/jetstream"), 0700); err != nil {
		t.Fatal(err)
	}
	var mainPID atomic.Int32
	var configHash c.Digest
	i.mainProcess = func(target infrahost.UnitTarget, d c.Deadline) (c.Process, error) {
		if target != infrahost.UnitNATS {
			return c.Process{}, c.ErrIncomplete
		}
		return infrahost.ReadProcess(mainPID.Load(), d)
	}
	// The namespace owns an unused private store and one actual launcher.
	// Observe that supervised process from /proc; the process set and digest
	// change at exec. This is not a host-wide systemd/nft readiness receipt.
	i.census = func(_ infrahost.CensusScope, d c.Deadline) (c.Census, error) {
		process, err := infrahost.ReadProcess(mainPID.Load(), d)
		if err != nil {
			return c.Census{}, err
		}
		snapshot := c.Census{Complete: true, Generation: 1, Processes: []c.Process{process}}
		data, _ := json.Marshal(snapshot)
		snapshot.Digest = sha256.Sum256(data)
		return snapshot, nil
	}
	i.initial = func(d c.Deadline) (infrahost.HostState, error) {
		store, err := i.serviceDirectory()
		if err != nil {
			return infrahost.HostState{}, err
		}
		defer closeTestResource(t, store)
		locks, err := i.files.Directory(stateDirectory+"/locks", false, 0, 0)
		if err != nil {
			return infrahost.HostState{}, err
		}
		defer closeTestResource(t, locks)
		guard, err := infralaunch.AcquireStore(store, locks)
		if err != nil {
			return infrahost.HostState{}, err
		}
		identity := guard.Identity()
		closeTestResource(t, guard)
		config, _ := json.Marshal(struct{ Device, Inode uint64 }{identity.LockDevice, identity.LockInode})
		configHash = sha256.Sum256(config)
		if err := i.files.Replace(stateDirectory+"/configs/"+digestName(configHash), config, 0400, i.gid); err != nil {
			return infrahost.HostState{}, err
		}
		if err := i.files.Replace(stateDirectory+"/binary-"+digestName(candidate.BinarySHA256), stock, 0755, i.gid); err != nil {
			return infrahost.HostState{}, err
		}
		deadline, err := controlbudget.After(20 * time.Second)
		if err != nil {
			return infrahost.HostState{}, err
		}
		state := infrahost.HostState{StartupFence: true, LaunchDeadline: deadline}
		state.Slots[c.NATSBinary].Active = candidate
		return state, nil
	}
	if _, err := i.bootstrap(rootDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	manager.units[infrahost.UnitNATS] = managedPolicy(infrahost.UnitPolicy{Target: infrahost.UnitNATS, Legacy: true, Expected: manager.units[infrahost.UnitNATS]}).Expected
	root, err := i.worker()
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, root)
	// Seed only the preceding closed fence, then drive ReleaseStart through the
	// ordinary authenticated client. Its real journal CAS produces the permit
	// queried and consumed by the separate launcher process below.
	policy := root.files.(*installedRecords).policy
	_, head, err := root.journal.Read(rootDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	permitDeadline, err := controlbudget.After(20 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	release := c.HostRequest{Version: 1, Op: c.ReleaseStart, Component: c.NATS, Tx: c.ID{1}, Step: 1, Sequence: 1, Domain: policy.Domain, PolicyRevision: policy.Revision, KeyID: "local-infra-v1", HostBootID: i.boot, Nonce: c.ID{5}, Expected: knownProcess(i.boot), ExpectedJournal: head, Candidate: candidate, Config: configHash, BeforeCensus: digest("empty private store namespace"), RemainingNS: uint64(10 * time.Second)}
	socketPath, err := os.MkdirTemp("", "root-exec-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(socketPath); err != nil {
			t.Error(err)
		}
	}()
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(socketPath, "control.sock"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close() //nolint:errcheck // Worker cleanup closes and checks this listener first; this fallback covers earlier setup failures.
	if err := os.Chmod(filepath.Join(socketPath, "control.sock"), 0660); err != nil {
		t.Fatal(err)
	}
	endpoint, err := os.Open(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, endpoint)
	credentials, err := i.files.Directory(credentialDirectory, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, credentials)
	local, err := infrapriv.ReadCredentialStore(credentials, infrapriv.LocalInfra, uint32(os.Geteuid()), uint32(os.Getegid()))
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, local)
	metadata, err := i.files.Open(credentialDirectory+"/launch.json", 0640)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, metadata)
	limit, err := infrapriv.NewPinnedWorkerLimit(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, limit)
	deadline, err := controlbudget.After(15 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	joined := make(chan struct{})
	var workers sync.WaitGroup
	go func() {
		defer close(joined)
		for n := 0; n < 5; n++ {
			conn, err := listener.AcceptUnix()
			if err != nil {
				break
			}
			workers.Add(1)
			go func() { defer workers.Done(); _ = root.ServeConn(conn, limit, deadline, nil) }()
		}
		workers.Wait()
	}()
	defer func() { closeTestResource(t, listener); closeTestResource(t, root); <-joined }()
	cmd := exec.Command(executable, "-test.run=^TestInstalledRootJournalsAuthenticatedSamePIDExec$", "-test.count=1")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "INVOCATION_ID=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "_INFRA_ROOT_EXEC_CHILD=1", "INVOCATION_ID="+strings.Repeat("b", 32), "GORACE=atexit_sleep_ms=0")
	cmd.ExtraFiles = []*os.File{credentials, endpoint, metadata}
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close() //nolint:errcheck // Wait closes this pipe; this fallback covers failures before StartProcess.
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close() //nolint:errcheck // Wait closes this pipe; this fallback covers failures before StartProcess.
	var diagnostic bytes.Buffer
	cmd.Stderr = &diagnostic
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = wait() }()
	mainPID.Store(int32(cmd.Process.Pid))
	fresh, err := i.census(infrahost.CensusScope{}, deadline)
	if err != nil {
		t.Fatal(err)
	}
	release.BeforeCensus = fresh.Digest
	evidence := effectRecord{Version: 1, Purpose: "tether-infra-effect-v1", Tx: release.Tx, Domain: release.Domain, Step: release.Step, Op: release.Op, Expected: release.Expected, Candidate: candidate, Config: configHash, BeforeCensus: release.BeforeCensus, Deadline: permitDeadline}
	evidenceBytes, _ := json.Marshal(evidence)
	release.Payload = sha256.Sum256(evidenceBytes)
	if err := i.files.Replace(stateDirectory+"/records/"+digestName(release.Payload)+".json", evidenceBytes, 0600, i.gid); err != nil {
		t.Fatal(err)
	}
	signing, err := c.SigningBytes(release)
	if err != nil {
		t.Fatal(err)
	}
	release.Signature, err = local.Sign(signing)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := requestDigest(release)
	if err != nil {
		t.Fatal(err)
	}
	projection := unitProjection{Version: 1, Purpose: "tether-infra-unit-projection-v1", Request: release, Intent: digest, Fence: infrahost.StartupFence{Tx: release.Tx, Domain: release.Domain, Step: 1, Component: c.NATS, Revision: 1, Closed: true, Deadline: permitDeadline}}
	if err := i.files.Encode(stateDirectory+"/unit-nats.json", projection, 0600, i.gid); err != nil {
		t.Fatal(err)
	}
	manager.start = func(target infrahost.UnitTarget) error {
		if target != infrahost.UnitNATS {
			return c.ErrIncomplete
		}
		return nil
	}
	client, err := infrapriv.NewPinnedClient(endpoint, uint32(os.Geteuid()), uint32(os.Getegid()))
	if err != nil {
		t.Fatal(err)
	}
	if receipt, files, err := client.Exchange(release, nil, deadline, nil); err != nil || !receipt.Applied || len(files) != 0 {
		t.Fatal("ordinary client did not commit the launch permit", err)
	}
	// A valid launch key cannot lend this parent's SO_PEERCRED to the actual
	// launcher child. Rejection must precede even a journal intent, not merely
	// prevent exec after a foreign caller has consumed the single-use permit.
	_, beforeDenied, err := root.journal.Read(deadline)
	if err != nil {
		t.Fatal(err)
	}
	bad := release
	bad.Op, bad.Sequence, bad.Nonce, bad.KeyID, bad.Expected, bad.ExpectedJournal, bad.BeforeCensus = c.LaunchClaim, 2, c.ID{6}, "local-launch-v1", fresh.Processes[0], beforeDenied, c.Digest{}
	launchKey, err := infrapriv.ReadCredentialStore(credentials, infrapriv.LocalLaunch, uint32(os.Geteuid()), uint32(os.Getegid()))
	if err != nil {
		t.Fatal(err)
	}
	bad.Signature = nil
	encoded, err := c.SigningBytes(bad)
	if err != nil {
		closeTestResource(t, launchKey)
		t.Fatal(err)
	}
	bad.Signature, err = launchKey.Sign(encoded)
	closeTestResource(t, launchKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, files, err := client.Exchange(bad, nil, deadline, nil); !errors.Is(err, infrapriv.ErrPermission) || len(files) != 0 {
		t.Fatal("foreign peer borrowed launch authority", err)
	}
	if _, afterDenied, err := root.journal.Read(deadline); err != nil || afterDenied != beforeDenied {
		t.Fatal("foreign peer changed the journal", err)
	}
	if _, err := input.Write([]byte("g")); err != nil {
		t.Fatal(err)
	}
	var witness struct {
		PID   int
		Ticks uint64
		SHA   [32]byte
		Refs  int
		Race  bool
	}
	if err := json.NewDecoder(output).Decode(&witness); err != nil {
		_ = cmd.Process.Kill()
		_ = wait()
		t.Fatalf("same-PID exec failed: %v %s", err, diagnostic.String())
	}
	query := c.HostRequest{Op: c.LaunchAck, Tx: c.ID{1}, Step: 1, Component: c.NATS}
	var ack infrahost.JournalEvent
	for {
		ack, err = root.journal.OperationRecord(query, deadline)
		if err == nil && ack.Phase == infrahost.EffectApplied {
			break
		}
		if _, err := controlbudget.Remaining(deadline); err != nil {
			t.Fatal("post-exec Ack never committed", err)
		}
		time.Sleep(5 * time.Millisecond)
	} // sleep-fixture: wait for the independent worker's durable Ack.
	claim := query
	claim.Op = c.LaunchClaim
	claimed, err := root.journal.OperationRecord(claim, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Phase != infrahost.EffectApplied || witness.PID != int(claimed.Request.Expected.PID) || witness.Ticks != claimed.Request.Expected.StartTicks || witness.SHA != candidate.BinarySHA256 || witness.Refs != 1 || !witness.Race || !sameRootInvocation(claimed.Request.Expected, ack.Request.Expected) {
		t.Fatal("journal and actual successor identity diverged")
	}
	if state, _, err := root.journal.Read(deadline); err != nil || state.Permit != (c.Digest{}) || state.LaunchClaimed {
		t.Fatal("Ack did not retire the single-use permit", err)
	}
	if _, err := input.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := wait(); err != nil {
		t.Fatalf("successor failed: %v %s", err, diagnostic.String())
	}
}
