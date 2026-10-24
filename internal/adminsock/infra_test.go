// Real Unix listeners, strict decoders and leak accounting; the Service below
// records dispatch and deadline custody only. It does not sign a real root
// effect, production readiness or previous-artifact hot-update completion.
package adminsock

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

type infraServiceRecorder struct {
	status                   c.Status
	err                      error
	starts, resumes, queries int
	deadline                 controlbudget.Deadline
	stop                     c.Stop
}

type infraLegacyRecorder struct{ calls atomic.Int32 }

var infraAdjacentArtifactsRequired = flag.Bool("infra-adjacent-artifacts", false, "require reviewed current/previous infra artifacts in the dedicated integration job")

func (f *infraLegacyRecorder) HandleCluster(r Request) Response {
	f.calls.Add(1)
	return Response{Op: r.Op, Code: CodeBadRequest, Error: "unexpected legacy dispatch"}
}

// The dedicated integration job deliberately fails if either reviewed artifact
// is missing. It must be selected separately from pure T46 checks; a fake old
// decoder or a bare Skip cannot establish any N-1 quadrant.
// Real broker and CLI children use the shared supervisor before Start. Private
// homes, seed directories and stores isolate every operation from production.
// origin: hot-update S9 plan §7.1 / §9.2 / T46
// origin: S10 internal review FINAL-REAL-FAILS F2
func TestInfraWireAdjacentArtifactQuadrants(t *testing.T) {
	// S9 §9.1 keeps reviewed external binaries out of default hermetic gates.
	// Record missing supply as NOT_RUN; partially supplied or invalid artifacts
	// still fail below so a dedicated job cannot hide a broken supply manifest.
	if !*infraAdjacentArtifactsRequired && os.Getenv("TETHER_S9_PREVIOUS_BINARY") == "" && os.Getenv("TETHER_S9_PREVIOUS_SHA256") == "" &&
		os.Getenv("TETHER_S9_CURRENT_BINARY") == "" && os.Getenv("TETHER_S9_CURRENT_SHA256") == "" {
		// SETUP-RED, never a skip (TestMissingDependencyNeverSkips).
		t.Fatal("SETUP-RED: NOT_RUN pending S9 P26/P30: reviewed current/previous artifacts are required (S9 plan §9.1)")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("NOT_RUN: adjacent runtime identity fixture requires linux")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInfraArtifactQuadrants(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra adjacent artifacts", beforeG)
	testharness.AssertNoFDLeak(t, "infra adjacent artifacts", beforeFD, 0)
}

func exerciseInfraArtifactQuadrants(t *testing.T) {
	t.Helper()
	binaries := map[string]string{}
	for _, generation := range []string{"PREVIOUS", "CURRENT"} {
		path := os.Getenv("TETHER_S9_" + generation + "_BINARY")
		sha := os.Getenv("TETHER_S9_" + generation + "_SHA256")
		if !filepath.IsAbs(path) || sha == "" {
			t.Fatalf("NOT_RUN pending P26/P30: reviewed %s absolute artifact path and SHA256 are required", generation)
		}
		if !strings.HasPrefix(filepath.Clean(path), "/home/weiland/s9wt/receipts/") {
			t.Fatal("adjacent artifacts must be copied into the private S9 receipt area")
		}
		// Check each ancestor before the next Lstat, so an alias cannot route
		// a supposedly private fixture to an installed production executable.
		parent := "/"
		for _, part := range strings.Split(strings.TrimPrefix(filepath.Clean(path), "/"), "/") {
			parent = filepath.Join(parent, part)
			info, err := os.Lstat(parent)
			if err != nil || info.Mode()&os.ModeSymlink != 0 {
				t.Fatal("adjacent fixture path is missing or contains a symlink")
			}
		}
		// Never resolve a name on PATH or accept the installed production binary.
		if strings.Contains(path, "/.local/bin/") || strings.Contains(path, "/.tether/") {
			t.Fatal("production binary/home is forbidden in the adjacent artifact fixture")
		}
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("reviewed artifact is missing or not a regular file: %v", err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		actual := sha256.Sum256(data)
		if hex.EncodeToString(actual[:]) != sha {
			t.Fatal("reviewed artifact SHA256 mismatch")
		}
		binaries[generation] = path
	}
	if binaries["PREVIOUS"] == binaries["CURRENT"] || os.Getenv("TETHER_S9_PREVIOUS_SHA256") == os.Getenv("TETHER_S9_CURRENT_SHA256") {
		t.Fatal("N-1 fixture supplied the same artifact twice")
	}
	for _, brokerGeneration := range []string{"PREVIOUS", "CURRENT"} {
		exerciseInfraArtifactBroker(t, binaries, brokerGeneration)
	}
}

func exerciseInfraArtifactBroker(t *testing.T, binaries map[string]string, generation string) {
	t.Helper()
	root := testharness.GuardProcessDirectory(t, t.TempDir())
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer account.Wipe()
	broker, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Wipe()
	accountPub, _ := account.PublicKey()
	brokerPub, _ := broker.PublicKey()
	permissions := auth.PermissionsForInfraBroker()
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: true, StoreDir: filepath.Join(root, "js"),
		Nkeys:       []*server.NkeyUser{{Nkey: brokerPub, Permissions: &server.Permissions{Publish: &server.SubjectPermission{Allow: permissions.Pub.Allow}, Subscribe: &server.SubjectPermission{Allow: permissions.Sub.Allow}}}},
		AuthCallout: &server.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{brokerPub}}})
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	defer func() { ns.Shutdown(); ns.WaitForShutdown() }()
	if !ns.ReadyForConnections(3 * time.Second) {
		t.Fatal("adjacent NATS fixture is not ready")
	}
	seeds := filepath.Join(root, "seeds")
	if err := os.Mkdir(seeds, 0700); err != nil {
		t.Fatal(err)
	}
	for name, key := range map[string]nkeys.KeyPair{"account.nk": account, "broker.nk": broker} {
		seed, err := key.Seed()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(seeds, name), seed, 0600); err != nil {
			t.Fatal(err)
		}
	}
	socket := filepath.Join(root, "admin.sock")
	command := exec.Command(binaries[generation], "serve", "--db", filepath.Join(root, "state.db"), "--nats-url", ns.ClientURL(), "--tunnel-addr", "", "--admin-socket", socket, "--public-host", "test.local", "--auth-callout-seeds-dir", seeds)
	command.Env = infraArtifactEnvironment(root)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	log, err := os.OpenFile(filepath.Join(root, "broker.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	command.Stdout, command.Stderr = log, log
	wait, err := testharness.StartProcess(command)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = wait(); close(done) }()
	defer func() {
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			<-done
		}
	}()
	client := &Client{Path: socket, Timeout: time.Second}
	var before *Response
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		before, err = client.Call(Request{Op: OpRuntime})
		if err == nil && before.OK && before.Runtime != nil {
			break
		}
		select {
		case <-done:
			t.Fatalf("adjacent broker exited before readiness: %v", waitErr)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if err != nil || before == nil || !before.OK || before.Runtime == nil {
		t.Fatal("adjacent broker did not serve ordinary admin runtime")
	}
	image := filepath.Join("/proc", fmt.Sprint(command.Process.Pid), "exe")
	running, err := os.Stat(image)
	if err != nil {
		t.Fatal(err)
	}
	runningBytes, err := os.ReadFile(image)
	if err != nil {
		t.Fatal(err)
	}
	runningSHA := sha256.Sum256(runningBytes)
	if hex.EncodeToString(runningSHA[:]) != os.Getenv("TETHER_S9_"+generation+"_SHA256") {
		t.Fatal("actual broker image differs from the reviewed artifact")
	}
	for _, callerGeneration := range []string{"PREVIOUS", "CURRENT"} {
		caller := exec.Command(binaries[callerGeneration], "admin", "runtime", "--socket", socket, "--json")
		caller.Env = infraArtifactEnvironment(root)
		output, err := testharness.CombinedOutput(caller)
		if err != nil || !strings.Contains(string(output), "admin_runtime") {
			t.Fatalf("ordinary %s caller -> %s broker quadrant failed: %v", callerGeneration, generation, err)
		}
		caller = exec.Command(binaries[callerGeneration], "admin", "infra", "status", "--socket", socket, "--tx", strings.Repeat("01", 16))
		caller.Env = infraArtifactEnvironment(root)
		output, err = testharness.CombinedOutput(caller)
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatal("unenabled/old infrastructure capability unexpectedly succeeded")
		}
		if callerGeneration == "CURRENT" {
			if exit.ExitCode() != 64 && exit.ExitCode() != 75 {
				t.Fatalf("current caller did not explicitly refuse unsupported/unenabled capability: rc=%d", exit.ExitCode())
			}
			if !strings.Contains(string(output), "infra_unsupported") && !strings.Contains(string(output), "infra_not_enabled") {
				t.Fatal("current caller did not identify the missing infra capability")
			}
		} else if !strings.Contains(string(output), "unknown command") && !strings.Contains(string(output), "unsupported") {
			t.Fatal("old caller did not explicitly reject an unknown infra operation")
		}
	}
	after, err := client.Call(Request{Op: OpRuntime})
	current, statErr := os.Stat(image)
	if err != nil || !after.OK || after.Runtime == nil || after.Runtime.UptimeSeconds < before.Runtime.UptimeSeconds || statErr != nil || !os.SameFile(running, current) {
		t.Fatal("capability refusal restarted or reexeced the broker")
	}
	currentBytes, err := os.ReadFile(image)
	if err != nil || sha256.Sum256(currentBytes) != runningSHA {
		t.Fatal("capability refusal changed the running broker SHA256")
	}
}

func infraArtifactEnvironment(root string) []string {
	var environment []string
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		if name == "HOME" || strings.HasPrefix(name, "TETHER_") || strings.HasPrefix(name, "_TETHER_") {
			continue
		}
		environment = append(environment, value)
	}
	return append(environment, "HOME="+root, "TETHER_HOME="+root)
}

func (f *infraServiceRecorder) Start(p c.Plan, d c.Deadline, stop c.Stop) (c.Status, error) {
	f.starts++
	f.deadline, f.stop = d, stop
	return f.status, f.err
}
func (f *infraServiceRecorder) Resume(tx c.ID, d c.Deadline, stop c.Stop) (c.Status, error) {
	f.resumes++
	f.deadline, f.stop = d, stop
	return f.status, f.err
}
func (f *infraServiceRecorder) Status(tx c.ID, d c.Deadline) (c.Status, error) {
	f.queries++
	f.deadline = d
	return f.status, f.err
}

func infraAdminFixture() (proto.InfraCommand, c.Plan, *infraServiceRecorder) {
	p := c.Plan{Tx: c.ID{1}, Domain: c.ID{2}, Step: 1, Component: c.NATS, Action: c.Upgrade, PolicyRevision: 1,
		Candidate: c.Artifact{Kind: c.NATSBinary, Version: "2.14.6", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: c.Digest{1}, BinarySHA256: c.Digest{2}, CatalogSHA256: c.Digest{3}},
		Previous:  c.Artifact{Kind: c.NATSBinary, Version: "2.14.5", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: c.Digest{4}, BinarySHA256: c.Digest{5}, CatalogSHA256: c.Digest{3}},
		Before:    c.Process{BootID: "boot", PID: 42, StartTicks: 9, InvocationID: "unit", ExeDevice: 1, ExeInode: 2, ExeSHA256: c.Digest{5}, Cgroup: "/fixture"},
		Store:     c.Store{Device: 1, Inode: 2, MountID: 3, LockDevice: 1, LockInode: 4}, ConfigSHA256: c.Digest{6}}
	r := proto.InfraCommand{Version: 1, Purpose: "infra-request-v1", Op: "start", Component: "nats", Action: "upgrade", TxID: hex.EncodeToString(p.Tx[:]), ToVersion: "2.14.6", ExpectSHA256: hex.EncodeToString(p.Candidate.BinarySHA256[:]), RemainingNS: uint64(time.Second)}
	current := p.Before
	current.ExeSHA256 = p.Candidate.BinarySHA256
	f := &infraServiceRecorder{status: c.Status{Tx: p.Tx, Step: 1, Phase: c.Completed, Revision: 1, Current: current, Census: c.Census{Complete: true, Generation: 1, LiveBusiness: 5, Digest: c.Digest{7}}, Proof: c.Digest{8}}}
	return r, p, f
}

// origin: hot-update S9 plan §7.1 / T46 / L:TestInfraAdminLifecycle
func TestInfraAdminLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInfraAdmin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra admin", beforeG)
	testharness.AssertNoFDLeak(t, "infra admin", beforeFD, 0)
}

func exerciseInfraAdmin(t *testing.T) {
	t.Helper()
	r, p, f := infraAdminFixture()
	path := filepath.Join(t.TempDir(), "admin.sock")
	b := &InfraBindings{Service: f, Plan: func(proto.InfraCommand) (c.Plan, error) { return p, nil }, Capability: func() *proto.InfraCapability { return &proto.InfraCapability{Version: 1, NATS: true} }}
	legacy := &infraLegacyRecorder{}
	s := New(path, Backend{Infra: b, Cluster: legacy})
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: hermetic admin lifetime, explicitly joined.
	defer cancel()
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for _, filename := range []string{filepath.Dir(path), path} {
		info, err := os.Stat(filename)
		if err != nil {
			t.Fatal(err)
		}
		want := os.FileMode(0600)
		if filename == filepath.Dir(path) {
			want = 0700
		}
		if info.Mode().Perm() != want {
			t.Fatalf("admin boundary mode=%o, want %o", info.Mode().Perm(), want)
		}
	}
	client := &Client{Path: path, Timeout: time.Second}
	resp, err := client.Call(Request{Op: OpInfraUpgrade, Infra: &r})
	if err != nil || resp == nil || !resp.OK || resp.Infra == nil || resp.Infra.Report == nil || resp.Infra.Report.LiveBusiness != 5 {
		t.Fatalf("independent admin start not reachable: %+v %v", resp, err)
	}
	query := proto.InfraCommand{Version: 1, Purpose: "infra-request-v1", Op: "status", TxID: r.TxID, RemainingNS: uint64(time.Second)}
	resp, err = client.Call(Request{Op: OpInfraStatus, Infra: &query})
	if err != nil || !resp.OK || resp.Infra.Report.TxID != r.TxID {
		t.Fatalf("status lost transaction: %+v %v", resp, err)
	}
	query.Op = "recover"
	resp, err = client.Call(Request{Op: OpInfraRecover, Infra: &query})
	if err != nil || !resp.OK {
		t.Fatalf("recover not dispatched: %+v %v", resp, err)
	}
	query.Op, query.TxID = "probe", ""
	resp, err = client.Call(Request{Op: OpInfraProbe, Infra: &query})
	if err != nil || !resp.OK || resp.Infra.Capability == nil || !resp.Infra.Capability.NATS {
		t.Fatalf("wired probe not dispatched: %+v %v", resp, err)
	}
	// The wire carries no legacy ToVersion operand outside the infra envelope.
	for _, wire := range []string{
		`{"op":"broker_upgrade_reload","infra":null}`,
		`{"op":"broker_upgrade_reload","infra":` + string(mustAdminInfraJSON(t, r)) + `}`,
		`{"op":"infra_upgrade","infra":` + string(mustAdminInfraJSON(t, r)) + `,"to_version":"legacy"}`,
		`{"op":"infra_upgrade","infra":null}`,
		`{"op":"infra_upgrade","op":"broker_upgrade_reload","infra":` + string(mustAdminInfraJSON(t, r)) + `}`,
		strings.Replace(`{"op":"infra_upgrade","infra":`+string(mustAdminInfraJSON(t, r))+`}`, `"version":1`, `"version":1,"version":1`, 1),
	} {
		got := infraRawAdminCall(t, path, wire)
		if got.OK || got.Error == "" {
			t.Fatalf("ambiguous admin request admitted: %s", wire)
		}
	}
	// This response establishes framing, not a successful infrastructure update.
	if f.starts != 1 || f.resumes != 1 || f.queries != 1 {
		t.Fatalf("unexpected effect dispatch: %+v", f)
	}
	if legacy.calls.Load() != 0 {
		t.Fatal("infra operand reached the legacy effect callback")
	}
	// Partially framed clients are real accepted handlers and must be joined by
	// Close while the server object remains live; test exit cannot reclaim them.
	partial, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = partial.Close() }()
	if _, err := partial.Write([]byte(`{"op":"infra_upgrade",`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal admin retained socket: %v", err)
	}
	if _, err := client.Call(Request{Op: OpInfraStatus, Infra: &query}); err == nil {
		t.Fatal("closed admin accepted work")
	}
}

func mustAdminInfraJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func infraRawAdminCall(t *testing.T, path, wire string) Response {
	t.Helper()
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte(wire + "\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.Unmarshal(line, &response); err != nil {
		t.Fatal(err)
	}
	return response
}

// origin: hot-update S9 plan §7.1 / T46 / M23
func TestInfraAdminNeverFallsThroughToLegacyReload(t *testing.T) {
	r, _, _ := infraAdminFixture()
	s := New("", Backend{})
	for i, request := range []Request{{Op: OpInfraUpgrade, Infra: &r}, {Op: OpInfraUpgrade}, {Op: OpBrokerUpgradeReload, Infra: &r}, {Op: OpInfraUpgrade, Infra: &r, ToVersion: "legacy"}, {Op: OpInfraUpgrade, Infra: &r, Upgrade: &UpgradeRequest{Op: "upgrade.start"}}} {
		response := s.dispatch(request)
		code := "infra_config_invalid"
		if i == 0 {
			code = "infra_unsupported"
		}
		if response.OK || response.Code != code || response.Reloading {
			t.Fatalf("unwired/mixed infra request became legacy effect: %+v", response)
		}
	}
	if r, err := decodeInfraAdminRequest([]byte(`{"op":"runtime"}`)); err != nil || r.Op != OpRuntime || r.Infra != nil {
		t.Fatalf("legacy default changed: %+v %v", r, err)
	}
	if body := string(mustAdminInfraJSON(t, Request{Op: OpRuntime})); body != `{"op":"runtime"}` {
		t.Fatalf("legacy zero-extension shape changed: %s", body)
	}
	if body := string(mustAdminInfraJSON(t, Response{Op: OpRuntime, OK: true})); body != `{"op":"runtime","ok":true}` {
		t.Fatalf("legacy response changed: %s", body)
	}
	if response := fencedInfraAdminResponse(Request{Op: OpInfraUpgrade, Infra: &r}); response.OK || response.Code != "infra_busy" || response.Infra == nil {
		t.Fatal("fenced infra admission lost its independent envelope/code")
	}
}

// origin: hot-update S9 plan §2.3 / §7.2 / T46
func TestInfraAdminReportsUnknownAndRetainedResponsibilities(t *testing.T) {
	_, _, f := infraAdminFixture()
	for _, tc := range []struct {
		name string
		edit func(*c.Status)
		code string
	}{
		{"empty", func(s *c.Status) { *s = c.Status{} }, "infra_result_unknown"},
		{"old_owner", func(s *c.Status) { s.Census.OldOwnerPending = 1 }, "infra_recovery_required"},
		{"incomplete_census", func(s *c.Status) { s.Census.Complete = false }, "infra_recovery_required"},
		{"no_proof", func(s *c.Status) { s.Proof = c.Digest{} }, "infra_recovery_required"},
		{"no_running_image", func(s *c.Status) { s.Current.ExeSHA256 = c.Digest{} }, "infra_recovery_required"},
		{"halted", func(s *c.Status) { s.Phase = c.Halted }, "infra_recovery_required"},
		{"deferred", func(s *c.Status) { s.Phase = c.Deferred }, "infra_not_ready"},
		{"different_tx", func(s *c.Status) { s.Tx = c.ID{99} }, "infra_internal"},
		{"private_code", func(s *c.Status) { s.Code = "seed-canary" }, "infra_internal"},
		{"missing_step", func(s *c.Status) { s.Step = 0 }, "infra_recovery_required"},
		{"missing_revision", func(s *c.Status) { s.Revision = 0 }, "infra_recovery_required"},
		{"missing_census_generation", func(s *c.Status) { s.Census.Generation = 0 }, "infra_recovery_required"},
		{"missing_census_digest", func(s *c.Status) { s.Census.Digest = c.Digest{} }, "infra_recovery_required"},
		{"missing_boot", func(s *c.Status) { s.Current.BootID = "" }, "infra_recovery_required"},
		{"missing_pid", func(s *c.Status) { s.Current.PID = 0 }, "infra_recovery_required"},
		{"missing_start_ticks", func(s *c.Status) { s.Current.StartTicks = 0 }, "infra_recovery_required"},
		{"missing_invocation", func(s *c.Status) { s.Current.InvocationID = "" }, "infra_recovery_required"},
		{"missing_exe_device", func(s *c.Status) { s.Current.ExeDevice = 0 }, "infra_recovery_required"},
		{"missing_exe_inode", func(s *c.Status) { s.Current.ExeInode = 0 }, "infra_recovery_required"},
		{"missing_cgroup", func(s *c.Status) { s.Current.Cgroup = "" }, "infra_recovery_required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := f.status
			tc.edit(&status)
			response := infraAdminStatus(OpInfraUpgrade, f.status.Tx, status, nil)
			if response.OK || response.Code != tc.code {
				t.Fatalf("false completion: %+v", response)
			}
			if strings.Contains(string(mustAdminInfraJSON(t, response)), "seed-canary") {
				t.Fatal("private host error escaped")
			}
			if tc.name == "empty" && (response.Infra.Report == nil || response.Infra.Report.TxID != hex.EncodeToString(f.status.Tx[:]) || response.Infra.Report.Phase != "unknown") {
				t.Fatal("unknown result lost the original status query identity")
			}
		})
	}
	status := f.status
	status.Phase, status.Detail, status.Current.Cgroup = c.Halted, "key-canary", "key-canary"
	response := infraAdminStatus(OpInfraStatus, status.Tx, status, nil)
	if !response.OK || response.Infra.Report.Phase != "halted" {
		t.Fatalf("status cannot inspect halted transaction: %+v", response)
	}
	response = infraAdminStatus(OpInfraRecover, status.Tx, status, errors.New("key-canary"))
	if response.OK || response.Code != "infra_internal" || strings.Contains(string(mustAdminInfraJSON(t, response)), "key-canary") {
		t.Fatal("private error leaked or became recovery success")
	}
	var runtimeReport RuntimeReport
	SetInfraRuntimeReport(&runtimeReport, &proto.InfraCapability{Version: 1, NATS: true}, &status)
	if strings.Contains(string(mustAdminInfraJSON(t, runtimeReport)), "key-canary") {
		t.Fatal("runtime report leaked private state")
	}
	SetInfraRuntimeReport(&runtimeReport, nil, nil)
	if runtimeReport.Infra != nil || runtimeReport.InfraCapability != nil {
		t.Fatal("unmeasured runtime retained readiness")
	}
}

// origin: hot-update S9 plan §7.1 / T46
func TestInfraAdminBindsResolvedPlanAndOriginalBudget(t *testing.T) {
	r, p, f := infraAdminFixture()
	for _, edit := range []func(*c.Plan){
		func(p *c.Plan) { p.Tx = c.ID{99} },
		func(p *c.Plan) { p.Component = c.HostSupport },
		func(p *c.Plan) { p.Candidate.Version = "2.14.5" },
		func(p *c.Plan) { p.Candidate.BinarySHA256 = c.Digest{99} },
		func(p *c.Plan) { p.Action = c.Rollback },
		func(p *c.Plan) { p.Before = c.Process{} },
	} {
		changed := p
		edit(&changed)
		if validateInfraPlan(r, changed) == nil {
			t.Fatal("resolver substituted a signed/public plan selector")
		}
	}
	if err := validateInfraPlan(r, p); err != nil {
		t.Fatal(err)
	}
	before, err := controlbudget.Now()
	if err != nil {
		t.Fatal(err)
	}
	s := New("", Backend{Infra: &InfraBindings{Service: f, Plan: func(proto.InfraCommand) (c.Plan, error) { return p, nil }, Capability: func() *proto.InfraCapability { return &proto.InfraCapability{Version: 1, NATS: true} }}})
	response := dispatchInfra(s, Request{Op: OpInfraUpgrade, Infra: &r})
	after, err := controlbudget.Now()
	if err != nil || !response.OK || int64(f.deadline) < before+int64(r.RemainingNS) || int64(f.deadline) > after+int64(r.RemainingNS) {
		t.Fatalf("request did not retain one absolute budget: %+v %v", response, err)
	}
	if f.starts != 1 {
		t.Fatal("valid plan did not dispatch exactly once")
	}
	response = dispatchInfra(s, Request{Op: OpInfraUpgrade, Infra: &r, infraReceived: time.Now().Add(-2 * time.Second)})
	if response.OK || response.Code != "infra_budget_exceeded" || f.starts != 1 {
		t.Fatal("expired framing/admission wait refreshed the attempt budget")
	}
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: canceled admission fixture.
	cancel()
	s.readCtx = ctx
	if response := dispatchInfra(s, Request{Op: OpInfraUpgrade, Infra: &r}); response.OK || response.Code != "infra_budget_exceeded" || f.starts != 1 {
		t.Fatal("canceled admission dispatched an effect")
	}
	s.readCtx = nil
	for _, capability := range []*proto.InfraCapability{nil, {Version: 0, NATS: true}, {Version: 1}, {Version: 1, Caddy: true}} {
		command := proto.InfraCommand{Version: 1, Purpose: "infra-request-v1", Op: "probe", Component: "nats", RemainingNS: uint64(time.Second)}
		s.backend.Infra.Capability = func() *proto.InfraCapability { return capability }
		if response := dispatchInfra(s, Request{Op: OpInfraProbe, Infra: &command}); response.OK || response.Code != "infra_unsupported" {
			t.Fatal("unwired or different component inferred readiness")
		}
	}
}
