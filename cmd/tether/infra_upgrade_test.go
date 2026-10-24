//go:build linux

package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/infracli"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const infraTestTx = "1234567890abcdef1234567890abcdef"

// origin: hot-update S9 plan §7.1 / T52 / M26
func TestInfraCLISelectsComponentBeforeHostPlan(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "--to-version is required"},
		{[]string{"--broker-only"}, "--to-version is required"},
		{[]string{"--component", "caddy", "--status", infraTestTx}, "--account-seed is required"},
		{[]string{"--component", "nats", "--rollback"}, "--account-seed is required"},
		{[]string{"--component", "caddy", "--reload-config", "--config-sha256", strings.Repeat("b", 64)}, "--account-seed is required"},
		{[]string{"--component", "caddy", "--rollback", "--backup-taken"}, "infra_config_invalid"},
		{[]string{"--component", "nats", "--rollback", "--ack-writefence"}, "infra_config_invalid"},
		{[]string{"--component", "nats", "--rollback", "--broker-only"}, "infra_config_invalid"},
		{[]string{"--component", "nats", "--recover", infraTestTx}, "--account-seed is required"},
		{[]string{"--status", infraTestTx}, "infra_unsupported"},
		{[]string{"--status", ""}, "infra_config_invalid"},
	} {
		cmd := newClusterUpgradeCmd()
		cmd.SetContext(t.Context())
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs(append(tc.args, "--home", t.TempDir()))
		err := cmd.Execute()
		if err == nil || classifyExit(err) != 64 || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("args=%v: err=%v class=%d want=%q", tc.args, err, classifyExit(err), tc.want)
		}
	}
	root := newRootCmd()
	for _, path := range [][]string{{"admin", "infra", "upgrade"}, {"admin", "infra", "status"}, {"admin", "infra", "recover"}, {"admin", "infra", "rotate-policy"}, {"cluster", "upgrade"}} {
		cmd, rest, err := root.Find(path)
		if err != nil || len(rest) != 0 || cmd.Hidden {
			t.Fatalf("missing/hidden command %v: %v", path, err)
		}
		if cmd.Name() == "upgrade" {
			for _, name := range []string{"component", "rollback", "reload-config", "config-sha256"} {
				f := cmd.Flags().Lookup(name)
				if f == nil || f.Hidden {
					t.Fatalf("missing/hidden flag %v --%s", path, name)
				}
			}
		}
	}
	// The existing structural gate's dump format is the handoff format for
	// both golden owners; keep the exact new rows outside the product tree.
	for _, line := range strings.Split(dumpCommandTree(root), "\n") {
		if strings.HasPrefix(line, "tether admin infra") || strings.HasPrefix(line, "tether cluster upgrade\t") {
			t.Log(line)
		}
	}
	for code, want := range infracli.ExitClasses {
		if got := brokerCodeExitClass(code); got != want || refusalHint(code, "") == "" {
			t.Fatalf("central code/hint %s: %d want %d", code, got, want)
		}
		if got := classifyExit(infracli.Failure(code, "", infraTestTx, "halted")); got != want {
			t.Fatalf("central sink %s: %d want %d", code, got, want)
		}
	}
	policy := newAdminCmd()
	policy.SetContext(t.Context())
	policy.SetOut(io.Discard)
	policy.SetErr(io.Discard)
	policy.SetArgs([]string{"--socket", filepath.Join(t.TempDir(), "unopened.sock"), "infra", "rotate-policy"})
	err := policy.Execute()
	want := 64
	if os.Geteuid() != 0 {
		want = 77
	}
	if classifyExit(err) != want {
		t.Fatalf("unbound policy rotation did not refuse before effect: %v class=%d", err, classifyExit(err))
	}
}

// origin: hot-update S9 plan §7.1 / §7.3 / T53 / TestInfraCLILifecycle
func TestInfraCLIStatusJSONAndNativeExits(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "tether")
	build := exec.CommandContext(t.Context(), "go", "build", "-race", "-o", bin, ".")
	var output bytes.Buffer
	build.Stdout, build.Stderr = &output, &output
	wait, err := testharness.StartProcess(build)
	if err == nil {
		err = wait()
	}
	if err != nil {
		t.Fatalf("build real CLI: %v\n%s", err, &output)
	}
	home := t.TempDir()
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInfraCLIProcess(t, bin, home)
	}
	testharness.AssertNoGoroutineLeak(t, "infra CLI processes x5", g)
	testharness.AssertNoFDLeak(t, "infra CLI processes x5", fd, 0)
	// Real signed cluster status shares the Status schema with local admin;
	// its fixtures double the remote management boundary, not the CLI path.
	exerciseInfraClusterCLI(t, bin)
}

func infraProcessReply(t *testing.T, bin, home string, args []string, reply func(adminsock.Request) adminsock.Response) cliRun {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "admin.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	done := make(chan error, 1)
	go func() {
		// Probe then Start are separate one-shot admin calls. Status/recover
		// use just one; the listener is closed and the goroutine joined below.
		for {
			conn, err := listener.Accept()
			if err != nil {
				done <- nil
				return
			}
			var req adminsock.Request
			err = json.NewDecoder(conn).Decode(&req)
			if err == nil {
				err = json.NewEncoder(conn).Encode(reply(req))
			}
			_ = conn.Close()
			if err != nil {
				done <- err
				return
			}
		}
	}()
	result := runTransactionCLI(t, bin, home, append([]string{"admin", "--socket", socket, "infra"}, args...)...)
	_ = listener.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return result
}

func exerciseInfraCLIProcess(t *testing.T, bin, home string) {
	t.Helper()
	for _, phase := range []string{"halted", "deferred", "unknown"} {
		run := infraProcessReply(t, bin, home, []string{"status", "--tx", infraTestTx}, func(req adminsock.Request) adminsock.Response {
			if req.Infra == nil || req.Op != adminsock.OpInfraStatus || req.Infra.TxID != infraTestTx {
				return adminsock.Response{Code: "infra_config_invalid"}
			}
			return adminsock.Response{OK: true, Infra: &proto.InfraUpgradeResp{OK: true, Report: &proto.InfraReport{TxID: infraTestTx, Phase: phase, Code: "infra_result_unknown"}}}
		})
		var status c.Status
		if run.rc != 0 || run.stderr != "" || json.Unmarshal([]byte(run.stdout), &status) != nil || status.Phase != map[string]c.Phase{"halted": c.Halted, "deferred": c.Deferred, "unknown": 0}[phase] || fmt.Sprintf("%x", status.Tx) != infraTestTx {
			t.Fatalf("status %s rc=%d stdout=%q stderr=%q", phase, run.rc, run.stdout, run.stderr)
		}
	}
	for _, tc := range []struct {
		code string
		rc   int
	}{{"infra_not_ready", 75}, {"infra_artifact_invalid", 64}, {"infra_permission_denied", 77}, {"infra_recovery_required", 74}} {
		run := infraProcessReply(t, bin, home, []string{"recover", "--tx", infraTestTx}, func(req adminsock.Request) adminsock.Response {
			return adminsock.Response{Infra: &proto.InfraUpgradeResp{Code: tc.code, Report: &proto.InfraReport{TxID: infraTestTx, Phase: "halted", Code: tc.code}}}
		})
		if run.rc != tc.rc || run.stdout != "" || !strings.Contains(run.stderr, tc.code) || strings.Count(run.stderr, infraTestTx) != 1 || len(regexp.MustCompile(`\b[0-9a-f]{32}\b`).FindAllString(run.stderr, -1)) != 1 {
			t.Fatalf("native %s rc=%d out=%q err=%q", tc.code, run.rc, run.stdout, run.stderr)
		}
	}
	// A single-mode local upgrade reaches only the independent infra ops.
	for _, component := range []string{"nats", "caddy"} {
		run := infraProcessReply(t, bin, home, []string{"upgrade", "--component", component, "--rollback"}, func(req adminsock.Request) adminsock.Response {
			if req.Infra != nil && req.Op == adminsock.OpInfraProbe {
				return adminsock.Response{OK: true, Infra: &proto.InfraUpgradeResp{OK: true, Capability: &proto.InfraCapability{Version: 1, NATS: true, Caddy: true}}}
			}
			if req.Infra == nil || req.Op != adminsock.OpInfraUpgrade || req.Infra.Component != component || req.Infra.Action != "rollback" {
				return adminsock.Response{Infra: &proto.InfraUpgradeResp{Code: "infra_config_invalid"}}
			}
			return adminsock.Response{Infra: &proto.InfraUpgradeResp{Code: "infra_not_ready", Report: &proto.InfraReport{TxID: req.Infra.TxID, Phase: "deferred", Code: "infra_not_ready"}}}
		})
		if run.rc != 75 || !strings.Contains(run.stderr, "infra_not_ready") {
			t.Fatalf("single component %s rc=%d err=%q", component, run.rc, run.stderr)
		}
	}
}

func exerciseInfraClusterCLI(t *testing.T, bin string) {
	t.Helper()
	url := testharness.StartNATS(t)
	home, id := transactionCtlHome(t)
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer account.Wipe()
	seed, err := account.Seed()
	if err != nil {
		t.Fatal(err)
	}
	defer clear(seed)
	pub, err := account.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	seedPath := filepath.Join(home, "account.nk")
	if err := os.WriteFile(seedPath, seed, 0600); err != nil {
		t.Fatal(err)
	}
	roster, err := clusterroster.Build(seed, pub, 1, []proto.RosterBroker{{NodeID: "node-a", Phase: proto.RosterPhaseVoter}}, time.Now().Add(-time.Minute).UTC().Format(time.RFC3339), time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	requests := make(chan proto.InfraUpgradeReq, 16)
	for _, subject := range []string{proto.SubjCtrlClusterHealth(id.PublicKey), proto.SubjCtrlClusterRoster(id.PublicKey), proto.SubjClusterInfra("node-a")} {
		sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
			var response any
			switch m.Subject {
			case proto.SubjCtrlClusterHealth(id.PublicKey):
				response = proto.ClusterHealthResp{NodeID: "node-a", IsVoter: true, InfraCapability: &proto.InfraCapability{Version: 1, NATS: true, Caddy: true}}
			case proto.SubjCtrlClusterRoster(id.PublicKey):
				response = proto.ClusterManifest{SchemaVersion: 1, Roster: roster}
			default:
				req, err := proto.DecodeInfraUpgradeRequest(m.Data)
				if err != nil {
					response = proto.InfraUpgradeResp{Code: "infra_config_invalid"}
					break
				}
				body, err := proto.InfraUpgradeSigningBytes(req)
				sig, decodeErr := hex.DecodeString(req.Sig)
				if err != nil || decodeErr != nil || account.Verify(body, sig) != nil || req.Actor != id.PublicKey {
					response = proto.InfraUpgradeResp{Code: "infra_permission_denied"}
					break
				}
				requests <- req
				if req.Request.Op == "probe" {
					response = proto.InfraUpgradeResp{OK: true, Capability: &proto.InfraCapability{Version: 1, NATS: true, Caddy: true}}
					break
				}
				tx := req.Request.TxID
				if req.Request.Op == "status" && tx != infraTestTx {
					response = proto.InfraUpgradeResp{Code: "infra_result_unknown", Report: &proto.InfraReport{TxID: tx, Phase: "unknown", Code: "infra_result_unknown"}}
					break
				}
				response = proto.InfraUpgradeResp{OK: true, Report: &proto.InfraReport{TxID: tx, Phase: "halted", Code: "infra_recovery_required"}}
			}
			body, _ := json.Marshal(response)
			_ = m.Respond(body)
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Unsubscribe() }()
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, component := range []string{"nats", "caddy"} {
		run := runTransactionCLI(t, bin, home, "cluster", "upgrade", "--component", component, "--status", infraTestTx, "--account-seed", seedPath, "--nats-url", url, "--home", home)
		var status c.Status
		if run.rc != 0 || run.stderr != "" || json.Unmarshal([]byte(run.stdout), &status) != nil || status.Phase != c.Halted {
			t.Fatalf("cluster %s status rc=%d stdout=%q stderr=%q", component, run.rc, run.stdout, run.stderr)
		}
	}
	if len(requests) < 2 {
		t.Fatal("status never reached signed infra subject")
	}
	for len(requests) > 0 {
		if req := <-requests; req.Request.Op != "status" || req.Request.TxID != infraTestTx {
			t.Fatalf("status ran a broker/component leg: %+v", req.Request)
		}
	}
	for _, flags := range [][]string{
		{"--component", "nats", "--rollback"},
		{"--component", "nats", "--to-version", "2.14.6", "--expect-sha256", strings.Repeat("a", 64)},
		{"--component", "caddy", "--reload-config", "--config-sha256", strings.Repeat("b", 64)},
	} {
		args := append([]string{"cluster", "upgrade", "--account-seed", seedPath, "--nats-url", url, "--home", home}, flags...)
		run := runTransactionCLI(t, bin, home, args...)
		if run.rc != 74 || !strings.Contains(run.stderr, "infra_recovery_required") || len(regexp.MustCompile(`\b[0-9a-f]{32}\b`).FindAllString(run.stderr, -1)) != 1 {
			t.Fatalf("infra entered host planner or lost Tx: flags=%v rc=%d err=%q", flags, run.rc, run.stderr)
		}
		starts := 0
		for len(requests) > 0 {
			if r := <-requests; r.Request.Op == "start" {
				starts++
			}
		}
		if starts != 1 {
			t.Fatalf("infra start executed %d times", starts)
		}
	}
	for _, verb := range []string{"status", "recover"} {
		run := runTransactionCLI(t, bin, home, "cluster", "upgrade", "--component", "caddy", "--"+verb, strings.Repeat("f", 32), "--account-seed", seedPath, "--nats-url", url, "--home", home)
		if verb == "status" {
			var status c.Status
			if run.rc != 0 || json.Unmarshal([]byte(run.stdout), &status) != nil || status.Phase != 0 {
				t.Fatalf("unknown query rc=%d out=%q err=%q", run.rc, run.stdout, run.stderr)
			}
		} else if run.rc != 74 {
			t.Fatalf("unknown recover rc=%d err=%q", run.rc, run.stderr)
		}
	}
}
