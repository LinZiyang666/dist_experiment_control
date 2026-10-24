//go:build linux

// broker_role_test.go exercises validated role dispatch and pristine ownership.
// Real: same-PID exec, sealed handoff, pipe identities, FD flags and cleanup.
// Stubbed: no broker store/control runtime; full broker activation is P02/P11.
package agentboot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

const brokerRoleEnv = "_TETHER_BROKER_ROLE_TEST_"

type brokerRoleIdentity struct {
	Device uint64
	Inode  uint64
}

type brokerRoleOracle struct {
	PID      int
	Identity brokerRoleIdentity
	Args     []string
}

type brokerRoleReceipt struct {
	PID        int
	Role       string
	Generation uint64
	DataSHA256 string
	Identity   brokerRoleIdentity
	Hops       int
	Lost       bool
}

func init() {
	mode := os.Getenv(brokerRoleEnv + "MODE")
	if mode == "" {
		return
	}
	if err := runBrokerRoleHelper(mode); err != nil {
		fmt.Fprintln(os.Stderr, "BROKER_ROLE_ORACLE_REJECT:", err)
		os.Exit(1)
	}
	os.Exit(0)
}

// origin: hot-update S6 plan §4.1 / T03
func TestBrokerBootRoleOnce(t *testing.T) {
	if err := checkBrokerRoleWiring("../../cmd/tether/main.go", "../../cmd/tether/serve.go"); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"broker", agentimage.Role, "unknown", "bad-nonce"} {
		t.Run(role, func(t *testing.T) { exerciseBrokerRole(t, role) })
	}
}

// origin: hot-update S6 plan §11 / T57Boot
func TestBrokerBootLeakCycles(t *testing.T) {
	// Warm the subprocess poller before taking the exact descriptor baseline.
	exerciseBrokerRoleRedispatch(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	beforeIdentity := brokerRoleFDMultiset(t)
	for round := 0; round < 5; round++ {
		exerciseBrokerRoleRedispatch(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker boot role redispatch", beforeG)
	testharness.AssertNoFDLeak(t, "broker boot role redispatch", beforeFD, 0)
	if got := brokerRoleFDMultiset(t); !equalBrokerRoleFDs(beforeIdentity, got) {
		t.Fatalf("broker boot changed the FD identity multiset: before=%v after=%v", beforeIdentity, got)
	}
}

func exerciseBrokerRoleRedispatch(t *testing.T) {
	t.Helper()
	for _, role := range []string{"broker", agentimage.Role, "unknown", "bad-nonce"} {
		exerciseBrokerRole(t, role)
	}
}

func exerciseBrokerRole(t *testing.T, role string) {
	t.Helper()
	dir := t.TempDir()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close(); _ = writer.Close() }()
	identity, err := brokerRoleFileIdentity(writer)
	if err != nil {
		t.Fatal(err)
	}
	identityData, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "original-identity.json"), identityData, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^$")
	cmd.Env = brokerRoleEnvironment("dispatch", role, dir)
	cmd.ExtraFiles = []*os.File{writer}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	// Every exit path Waits; the bounded context kills the entire helper group.
	if err := cmd.Wait(); err != nil {
		t.Fatalf("role %s: %v\n%s", role, err, output.String())
	}
	data, err := os.ReadFile(filepath.Join(dir, "receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt brokerRoleReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.PID != cmd.Process.Pid || receipt.Identity != identity {
		t.Fatalf("role %s changed process or pipe identity: %+v", role, receipt)
	}
	pipeBytes, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if role == "unknown" || role == "bad-nonce" {
		if !receipt.Lost || receipt.Hops != 1 || len(pipeBytes) != 0 {
			t.Fatalf("unreadable dispatch escaped W1 cleanup: %+v bytes=%q", receipt, pipeBytes)
		}
		return
	}
	wantData, err := json.Marshal(brokerRoleOracle{cmd.Process.Pid, identity, []string{"tether", "serve", "--config", filepath.Join(dir, "broker.yaml")}})
	if err != nil {
		t.Fatal(err)
	}
	wantSHA := sha256.Sum256(wantData)
	if receipt.Lost || receipt.Role != role || receipt.Generation != 7 || receipt.Hops != 2 || receipt.DataSHA256 != hex.EncodeToString(wantSHA[:]) || string(pipeBytes) != "original pipe survived role dispatch\n" {
		t.Fatalf("pristine redispatch lost the original envelope or pipe: %+v bytes=%q", receipt, pipeBytes)
	}
}

func brokerRoleEnvironment(mode, role, dir string) []string {
	var env []string
	for _, entry := range handoff.CleanEnvironment(os.Environ()) {
		if strings.HasPrefix(entry, brokerRoleEnv) || strings.HasPrefix(entry, "HOME=") || strings.HasPrefix(entry, "TETHER_HOME=") || strings.HasPrefix(entry, "_TETHER_HANDOFF_ROLE=") {
			continue
		}
		env = append(env, entry)
	}
	return append(env, brokerRoleEnv+"MODE="+mode, brokerRoleEnv+"ROLE="+role, brokerRoleEnv+"DIR="+dir, "HOME="+dir, "TETHER_HOME="+dir)
}

func runBrokerRoleHelper(mode string) error {
	role, dir := os.Getenv(brokerRoleEnv+"ROLE"), os.Getenv(brokerRoleEnv+"DIR")
	if mode == "dispatch" {
		file := os.NewFile(3, "broker-role-original")
		defer func() { _ = file.Close() }()
		if _, err := unix.FcntlInt(3, unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
			return err
		}
		identity, err := brokerRoleFileIdentity(file)
		if err != nil {
			return err
		}
		oracle := brokerRoleOracle{os.Getpid(), identity, []string{"tether", "serve", "--config", filepath.Join(dir, "broker.yaml")}}
		data, err := json.Marshal(oracle)
		if err != nil {
			return err
		}
		// An untaken resource must be released while the receiver is alive;
		// process exit would otherwise conceal a missing Restored.Close.
		auxRead, auxWrite, err := os.Pipe()
		if err != nil {
			return err
		}
		defer func() { _ = auxRead.Close(); _ = auxWrite.Close() }()
		envelopeRole := role
		if role == "bad-nonce" {
			envelopeRole = "broker"
		}
		prepared, err := handoff.Prepare(handoff.State{Role: envelopeRole, Instance: "role-fixture", Generation: 7, Data: data}, []handoff.Resource{
			{ID: "original", Kind: "pipe", Files: map[string]*os.File{"writer": file}},
			{ID: "auxiliary", Kind: "pipe", Files: map[string]*os.File{"writer": auxWrite}},
		})
		if err != nil {
			return err
		}
		defer func() { _ = prepared.Close() }()
		// Both chooser hint and correct argv are absent in this same-PID hop.
		return prepared.Exec("/proc/self/exe", []string{"tether", "version", "--wrong-broker-argv"}, brokerRoleEnvironment("receive", role, dir))
	}
	if mode != "receive" && mode != "previous" {
		return errors.New("unknown role helper mode")
	}
	if role == "bad-nonce" {
		fd, nonce, _ := strings.Cut(os.Getenv("_TETHER_HANDOFF"), ":")
		if nonce == strings.Repeat("0", 64) {
			nonce = strings.Repeat("1", 64)
		} else {
			nonce = strings.Repeat("0", 64)
		}
		if err := os.Setenv("_TETHER_HANDOFF", fd+":"+nonce); err != nil {
			return err
		}
	}
	// Snapshot the inherited pipe before Begin, to prove cleanup while alive.
	var inherited brokerRoleIdentity
	var inheritedPipes []brokerRoleIdentity
	for _, saved := range mustBrokerRoleForeign() {
		var st unix.Stat_t
		if unix.Fstat(saved.fd, &st) == nil && st.Mode&unix.S_IFMT == unix.S_IFIFO {
			inherited = brokerRoleIdentity{uint64(st.Dev), st.Ino}
			inheritedPipes = append(inheritedPipes, inherited)
		}
	}
	img := Begin(nil)
	if img == nil || os.Getenv("_TETHER_HANDOFF") != "" {
		return errors.New("Begin did not claim and consume dispatch")
	}
	if second, err := handoff.Restore(); second != nil || err != nil {
		return errors.New("dispatch restored twice")
	}
	if role == "unknown" || role == "bad-nonce" {
		if img.Phase() != PhaseLost || img.CrashWindow() != CrashWindowHandoffRestore || Claim() != nil {
			return errors.New("unknown or invalid role was claimable")
		}
		if restored, err := TakeRole("broker"); restored != nil || err != nil {
			return errors.New("lost dispatch transferred to broker")
		}
		for _, entry := range mustBrokerRoleFDs() {
			for _, pipe := range inheritedPipes {
				if entry == pipe {
					return errors.New("W1 left an inherited pipe alive")
				}
			}
		}
		// Lost images no longer expose their manifest, so the parent supplies
		// the original pipe identity independently through its inherited fd 3
		// before the first exec (the opaque fixture receipt stays secret-free).
		originalData, err := os.ReadFile(filepath.Join(dir, "original-identity.json"))
		if err != nil {
			return err
		}
		if err := json.Unmarshal(originalData, &inherited); err != nil {
			return err
		}
		return writeBrokerRoleReceipt(dir, brokerRoleReceipt{PID: os.Getpid(), Identity: inherited, Lost: true, Hops: 1})
	}
	if restored, err := TakeRole("unsupported"); restored != nil || err == nil || img.Phase() != PhaseHeld {
		return errors.New("unsupported chooser consumed held ownership")
	}
	if role == agentimage.Role {
		if restored, err := TakeRole("broker"); restored != nil || err != nil || img.Phase() != PhaseHeld {
			return errors.New("broker chooser consumed an agent image")
		}
		if Claim() != img || Claim() != nil {
			return errors.New("agent image was not claimable exactly once")
		}
		if restored, err := TakeRole(agentimage.Role); restored != nil || err != nil {
			return errors.New("TakeRole duplicated a claimed agent owner")
		}
		return repackBrokerRole(dir, role, mode, img.restored)
	}
	if Claim() != nil {
		return errors.New("agent claimed a broker envelope")
	}
	restored, err := TakeRole("broker")
	if err != nil || restored == nil || img.Phase() != PhaseNone || Current() != nil || Claim() != nil {
		return errors.New("validated broker role did not transfer once")
	}
	defer func() { _ = restored.Close() }()
	if duplicate, err := TakeRole("broker"); duplicate != nil || err != nil {
		return errors.New("broker envelope transferred twice")
	}
	// Even a stale agent wrapper cannot abandon the transferred descriptor.
	img.Abandon(nil, CrashWindowNonDaemon, "wrong argv")
	return repackBrokerRole(dir, role, mode, restored)
}

func repackBrokerRole(dir, role, mode string, restored *handoff.Restored) error {
	var oracle brokerRoleOracle
	if err := json.Unmarshal(restored.State.Data, &oracle); err != nil {
		return err
	}
	if oracle.PID != os.Getpid() {
		return errors.New("redispatch changed PID")
	}
	if mode == "receive" {
		prepared, err := restored.Repack()
		if err != nil {
			return err
		}
		defer func() { _ = prepared.Close() }()
		// A failed exec closes only prepared copies; the envelope stays held.
		if err := prepared.Exec(filepath.Join(dir, "missing"), oracle.Args, brokerRoleEnvironment("previous", role, dir)); err == nil {
			return errors.New("missing redispatch artifact executed")
		}
		if err := prepared.Close(); err != nil {
			return err
		}
		prepared, err = restored.Repack()
		if err != nil {
			return err
		}
		defer func() { _ = prepared.Close() }()
		return prepared.Exec("/proc/self/exe", oracle.Args, brokerRoleEnvironment("previous", role, dir))
	}
	if strings.Join(os.Args, "\x00") != strings.Join(oracle.Args, "\x00") {
		return errors.New("previous image did not receive sealed correct argv")
	}
	file, err := restored.TakeFile("original", "writer")
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	identity, err := brokerRoleFileIdentity(file)
	if err != nil || identity != oracle.Identity {
		return errors.New("redispatch replaced original pipe")
	}
	if _, err := file.WriteString("original pipe survived role dispatch\n"); err != nil {
		return err
	}
	untaken := restored.Borrow()
	if len(untaken) != 1 || untaken[0].ID != "auxiliary" || len(untaken[0].Files) != 1 {
		return errors.New("untaken resource did not survive pristine repack")
	}
	auxiliary := untaken[0].Files["writer"]
	auxiliaryIdentity, err := brokerRoleFileIdentity(auxiliary)
	if err != nil {
		return err
	}
	if err := restored.Close(); err != nil {
		return err
	}
	if _, err := auxiliary.Stat(); err == nil {
		return errors.New("untaken role resource leaked while receiver alive")
	}
	if err := file.Close(); err != nil {
		return err
	}
	for _, live := range mustBrokerRoleFDs() {
		if live == identity || live == auxiliaryIdentity {
			return errors.New("role cleanup retained an inherited pipe alias")
		}
	}
	sum := sha256.Sum256(restored.State.Data)
	return writeBrokerRoleReceipt(dir, brokerRoleReceipt{PID: os.Getpid(), Role: restored.State.Role, Generation: restored.State.Generation, DataSHA256: hex.EncodeToString(sum[:]), Identity: identity, Hops: 2})
}

func writeBrokerRoleReceipt(dir string, receipt brokerRoleReceipt) error {
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "receipt.json"), data, 0o600)
}

func brokerRoleFileIdentity(file *os.File) (brokerRoleIdentity, error) {
	info, statErr := file.Stat()
	if statErr != nil {
		return brokerRoleIdentity{}, statErr
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return brokerRoleIdentity{}, errors.New("pipe has no kernel identity")
	}
	return brokerRoleIdentity{uint64(st.Dev), st.Ino}, nil
}

func mustBrokerRoleForeign() []foreignDescriptor {
	fds, err := foreignDescriptors()
	if err != nil {
		panic("role helper descriptor census failed")
	}
	return fds
}

func mustBrokerRoleFDs() []brokerRoleIdentity {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		panic("role helper FD inventory failed")
	}
	var identities []brokerRoleIdentity
	for _, entry := range entries {
		var fd int
		if _, err := fmt.Sscan(entry.Name(), &fd); err != nil || fd < 3 {
			continue
		}
		var stat unix.Stat_t
		if unix.Fstat(fd, &stat) == nil {
			identities = append(identities, brokerRoleIdentity{uint64(stat.Dev), stat.Ino})
		}
	}
	return identities
}

func brokerRoleFDMultiset(t *testing.T) map[brokerRoleIdentity]int {
	t.Helper()
	set := make(map[brokerRoleIdentity]int)
	for _, identity := range mustBrokerRoleFDs() {
		set[identity]++
	}
	return set
}

func equalBrokerRoleFDs(a, b map[brokerRoleIdentity]int) bool {
	if len(a) != len(b) {
		return false
	}
	for identity, count := range a {
		if b[identity] != count {
			return false
		}
	}
	return true
}

// gate-control: TestBrokerBootWiringControls
// This checks the P22 seam's source order independently of the real
// product/previous-image harness, which proves the resulting lifecycle.
// origin: hot-update S6 plan §4.1 / T03
func checkBrokerRoleWiring(mainPath, servePath string) error {
	fset := token.NewFileSet()
	mainFile, err := parser.ParseFile(fset, mainPath, nil, 0)
	if err != nil {
		return err
	}
	serveFile, err := parser.ParseFile(fset, servePath, nil, 0)
	if err != nil {
		return err
	}
	var calls = make(map[string][]token.Pos)
	collect := func(root ast.Node) {
		ast.Inspect(root, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				var name bytes.Buffer
				_ = format.Node(&name, fset, call.Fun)
				calls[name.String()] = append(calls[name.String()], call.Pos())
			}
			return true
		})
	}
	var mainBody *ast.BlockStmt
	for _, decl := range mainFile.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "main" {
			mainBody = fn.Body
		}
	}
	if mainBody == nil || len(mainBody.List) == 0 {
		return errors.New("missing main collector")
	}
	collect(mainBody)
	begin, take, from := calls["agentboot.Begin"], calls["agentboot.TakeRole"], calls["brokerimage.Boot"]
	if len(begin) != 1 || len(take) != 1 || len(from) != 1 || begin[0] < mainBody.List[0].Pos() || begin[0] > mainBody.List[0].End() || take[0] <= begin[0] || from[0] <= take[0] {
		return errors.New("bootstrap must Begin once and transfer validated role before dispatch")
	}
	if len(calls["handoff.Restore"]) != 0 || len(calls["brokerBoot.Finish"]) != 1 {
		return errors.New("second Restore or missing broker Finish funnel")
	}
	for _, name := range []string{"img.Abandon", "agent.BootUpgradeCheck", "isAgentDaemonInvocation"} {
		for _, pos := range calls[name] {
			if pos < take[0] {
				return errors.New("argv chooser abandoned the envelope before role transfer")
			}
		}
	}
	var guardErr error
	ast.Inspect(mainBody, func(node ast.Node) bool {
		guard, ok := node.(*ast.IfStmt)
		if !ok {
			return true
		}
		var body bytes.Buffer
		_ = format.Node(&body, fset, guard.Body)
		if strings.Contains(body.String(), "img.Abandon(") || strings.Contains(body.String(), "agent.BootUpgradeCheck(") {
			var cond bytes.Buffer
			_ = format.Node(&cond, fset, guard.Cond)
			if !strings.Contains(cond.String(), "restored == nil") {
				guardErr = errors.New("agent boot guard includes a transferred broker envelope")
			}
		}
		return true
	})
	if guardErr != nil {
		return guardErr
	}
	collect(serveFile)
	// The supported ephemeral lane is explicitly image-ineligible. Only this
	// exact cold-single guard may own an ordinary pool; an on-disk or restored
	// launch must still pass through the current store owner.
	// origin: hot-update S6 plan §3.1 / T03
	ast.Inspect(serveFile, func(node ast.Node) bool {
		guard, ok := node.(*ast.IfStmt)
		if !ok {
			return true
		}
		var cond bytes.Buffer
		_ = format.Node(&cond, fset, guard.Cond)
		if cond.String() != `!clusterMode && brokerBoot == nil && dbPath == ":memory:"` {
			return true
		}
		allowed := make(map[token.Pos]bool)
		ast.Inspect(guard.Body, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				var expr bytes.Buffer
				_ = format.Node(&expr, fset, call)
				if expr.String() == "storage.Open(dbPath)" || expr.String() == "cfg.DB.Close()" {
					allowed[call.Pos()] = true
				}
			}
			return true
		})
		for _, name := range []string{"storage.Open", "cfg.DB.Close"} {
			var retained []token.Pos
			for _, pos := range calls[name] {
				if !allowed[pos] {
					retained = append(retained, pos)
				}
			}
			calls[name] = retained
		}
		return true
	})
	for _, name := range []string{"storage.Open", "db.Close", "cfg.DB.Close", "b.SetReloadTrigger", "b.ReExecRequested", "reExecBrokerInPlace"} {
		if len(calls[name]) != 0 {
			return errors.New("serve retains a cold DB or post-Run exec owner")
		}
	}
	if len(calls["brokerimage.OpenSingleStore"]) != 1 || len(calls["brokerimage.CloseStoreOwner"]) != 1 {
		return errors.New("single store has no explicit constructor/terminal owner")
	}
	return nil
}

// origin: hot-update S6 plan §4.1 / T03
func TestBrokerBootWiringControls(t *testing.T) {
	mainSource, err := os.ReadFile("../../cmd/tether/main.go")
	if err != nil {
		t.Fatal(err)
	}
	serveSource, err := os.ReadFile("../../cmd/tether/serve.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"positive", "early-abandon", "double-restore", "missing-finish", "captured-db", "post-run-exec", "disk-cold-open", "restored-cold-open", "cluster-cold-open", "wrong-memory-path"} {
		t.Run(mutation, func(t *testing.T) {
			mainData, serveData := string(mainSource), string(serveSource)
			switch mutation {
			case "positive":
			case "early-abandon":
				mainData = strings.Replace(mainData, "restored, bootErr := agentboot.TakeRole", "img.Abandon(nil, \"non_daemon_invocation\", \"early\")\nrestored, bootErr := agentboot.TakeRole", 1)
			case "double-restore":
				mainData = strings.Replace(mainData, "restored, bootErr := agentboot.TakeRole", "handoff.Restore()\nrestored, bootErr := agentboot.TakeRole", 1)
			case "missing-finish":
				mainData = strings.Replace(mainData, "brokerBoot.Finish(err)", "brokerBoot.SkipFinish(err)", 1)
			case "captured-db":
				serveData += "\nfunc capturedDB() { db.Close() }\n"
			case "post-run-exec":
				serveData += "\nfunc postRunExec() { reExecBrokerInPlace(cmd) }\n"
			case "disk-cold-open":
				serveData = strings.Replace(serveData, `&& dbPath == ":memory:"`, "", 1)
			case "restored-cold-open":
				serveData = strings.Replace(serveData, `!clusterMode && brokerBoot == nil && dbPath`, `!clusterMode && dbPath`, 1)
			case "cluster-cold-open":
				serveData = strings.Replace(serveData, `!clusterMode && brokerBoot == nil && dbPath`, `brokerBoot == nil && dbPath`, 1)
			case "wrong-memory-path":
				serveData = strings.Replace(serveData, `storage.Open(dbPath)`, `storage.Open("state.db")`, 1)
			default:
				t.Fatal("unknown wiring control")
			}
			dir := t.TempDir()
			mainPath, servePath := filepath.Join(dir, "main.go"), filepath.Join(dir, "serve.go")
			if err := os.WriteFile(mainPath, []byte(mainData), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(servePath, []byte(serveData), 0o600); err != nil {
				t.Fatal(err)
			}
			err := checkBrokerRoleWiring(mainPath, servePath)
			if (mutation == "positive") != (err == nil) {
				t.Fatalf("wiring control %s: %v", mutation, err)
			}
		})
	}
}
