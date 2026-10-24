package infracli

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/spf13/cobra"
)

const testTx = "1234567890abcdef1234567890abcdef"

func classOf(err error) int {
	if err == nil {
		return 0
	}
	var e *ExitError
	if errors.As(err, &e) {
		return e.Class
	}
	return -1
}

func report(phase string) *proto.InfraReport {
	return &proto.InfraReport{TxID: testTx, Step: 1, Revision: 2, Phase: phase, RunningSHA256: strings.Repeat("a", 64), Proof: strings.Repeat("b", 64)}
}

// origin: hot-update S9 plan §7.3 / T53 / M26
func TestInfraCLIReportsUnknownAndDeferred(t *testing.T) {
	for _, tc := range []struct {
		code  string
		class int
	}{
		{"infra_unsupported", 64}, {"infra_artifact_invalid", 64}, {"infra_config_invalid", 64}, {"infra_incompatible", 64},
		{"infra_permission_denied", 77}, {"infra_policy_stale", 77},
		{"infra_not_enabled", 75}, {"infra_busy", 75}, {"infra_not_ready", 75}, {"infra_inventory_incomplete", 75}, {"infra_budget_exceeded", 75},
		{"infra_result_unknown", 74}, {"infra_recovery_required", 74}, {"infra_internal", 70},
	} {
		t.Run(tc.code, func(t *testing.T) {
			err := Failure(tc.code, "barrier detail", testTx, "halted")
			if classOf(err) != tc.class || !strings.Contains(err.Error(), tc.code) || !strings.Contains(err.Error(), "barrier detail") || strings.Count(err.Error(), testTx) != 1 || !strings.Contains(err.Error(), "status --tx <id>") {
				t.Fatalf("class/hint/Tx lost: %v (%d)", err, classOf(err))
			}
			if Hints[tc.code] == "" {
				t.Fatal("missing actionable hint")
			}
		})
	}
	if len(ExitClasses) != 14 || len(Hints) != 14 {
		t.Fatal("closed code family drift")
	}
	if err := Failure("external-secret-path", "secret", testTx, "unknown"); classOf(err) != 70 || !strings.Contains(err.Error(), "infra_internal") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unknown external error was not sanitized: %v", err)
	}
	for _, tc := range []struct {
		phase, code string
		want        int
	}{
		{"completed", "", 0}, {"prepared", "", 74}, {"retiring", "", 74}, {"halted", "", 74},
		{"halted", "infra_recovery_required", 74}, {"deferred", "", 75}, {"deferred", "infra_not_ready", 75}, {"unknown", "infra_result_unknown", 74},
	} {
		t.Run(tc.phase+tc.code, func(t *testing.T) {
			r := report(tc.phase)
			r.Code = tc.code
			resp := &proto.InfraUpgradeResp{OK: true, Report: r}
			var out bytes.Buffer
			err := Render(&out, proto.InfraCommand{Op: "recover", TxID: testTx}, resp)
			if classOf(err) != tc.want {
				t.Fatalf("cut phase=%s err=%v class=%d", tc.phase, err, classOf(err))
			}
			if tc.want != 0 && out.Len() != 0 {
				t.Fatalf("failed cut printed success: %s", &out)
			}
			out.Reset()
			if err := Render(&out, proto.InfraCommand{Op: "status", TxID: testTx}, resp); err != nil {
				t.Fatal(err)
			}
			var status c.Status
			if err := json.Unmarshal(out.Bytes(), &status); err != nil {
				t.Fatal(err)
			}
			if hex.EncodeToString(status.Tx[:]) != testTx || status.Phase != c.Phase(map[string]int{"completed": 9, "prepared": 1, "retiring": 8, "halted": 11, "deferred": 10, "unknown": 0}[tc.phase]) || status.Code != tc.code {
				t.Fatalf("status JSON lost observation: %s", &out)
			}
			var shape map[string]json.RawMessage
			if err := json.Unmarshal(out.Bytes(), &shape); err != nil || len(shape) != 10 || shape["Tx"] == nil || shape["Phase"] == nil {
				t.Fatalf("Status schema changed: %s", &out)
			}
		})
	}
	for _, mutate := range []func(*proto.InfraUpgradeResp){
		func(r *proto.InfraUpgradeResp) { r.Report.TxID = strings.Repeat("f", 32) },
		func(r *proto.InfraUpgradeResp) { r.Report.Phase = "new_phase" },
		func(r *proto.InfraUpgradeResp) { r.Report.Code = "foreign" },
		func(r *proto.InfraUpgradeResp) { r.Report.Proof = "bad" },
		func(r *proto.InfraUpgradeResp) { r.Report.RunningSHA256 = "bad" },
		func(r *proto.InfraUpgradeResp) { r.Report.Step = 0 },
		func(r *proto.InfraUpgradeResp) { r.Report.Revision = 0 },
		func(r *proto.InfraUpgradeResp) { r.Report.OldOwnerPending = 1 },
		func(r *proto.InfraUpgradeResp) { r.Report.Proof = "" },
		func(r *proto.InfraUpgradeResp) { r.Report.RunningSHA256 = "" },
		func(r *proto.InfraUpgradeResp) { r.OK = false },
	} {
		r := &proto.InfraUpgradeResp{OK: true, Report: report("completed")}
		mutate(r)
		if err := Render(io.Discard, proto.InfraCommand{Op: "start", TxID: testTx}, r); err == nil {
			t.Fatalf("malformed completion accepted: %+v", r)
		}
	}
	for _, resp := range []*proto.InfraUpgradeResp{nil, {Code: "infra_permission_denied"}, {OK: true}} {
		if err := Render(io.Discard, proto.InfraCommand{Op: "start", TxID: testTx}, resp); err == nil {
			t.Fatal("missing cut result accepted")
		}
	}
	var out bytes.Buffer
	if err := Render(&out, proto.InfraCommand{Op: "status", TxID: testTx}, &proto.InfraUpgradeResp{Code: "infra_result_unknown"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"Phase":0`) {
		t.Fatalf("unknown query: %s", &out)
	}
}

// origin: hot-update S9 plan §7.1 / T52 / M26
func TestInfraCLIValidatesClosedOperandsBeforeEffects(t *testing.T) {
	base := Options{Component: "nats", ToVersion: "2.14.6", ExpectSHA256: strings.Repeat("a", 64)}
	for _, change := range []func(*Options){
		func(o *Options) { o.Component = "" }, func(o *Options) { o.Component = "helper" },
		func(o *Options) { o.BrokerOnly = true }, func(o *Options) { o.AckWriteFence = true }, func(o *Options) { o.BackupTaken = true },
		func(o *Options) { o.Resume = testTx }, func(o *Options) { o.ToVersion = "" }, func(o *Options) { o.ExpectSHA256 = "" },
		func(o *Options) { o.ExpectSHA256 = strings.Repeat("A", 64) }, func(o *Options) { o.ExpectSHA256 = strings.Repeat("0", 64) },
		func(o *Options) { o.Rollback = true }, func(o *Options) { o.ReloadConfig = true }, func(o *Options) { o.Status = testTx }, func(o *Options) { o.Recover = testTx },
	} {
		o := base
		change(&o)
		called := false
		err := Execute(t.Context(), io.Discard, o, func(context.Context, proto.InfraCommand) (*proto.InfraUpgradeResp, error) {
			called = true
			return nil, nil
		})
		if classOf(err) != 64 || called {
			t.Fatalf("invalid operands reached effect: %+v err=%v called=%v", o, err, called)
		}
	}
	for _, o := range []Options{base, {Component: "nats", Rollback: true}, {Component: "caddy", ReloadConfig: true, ConfigSHA256: strings.Repeat("b", 64)}, {Component: "caddy", Status: testTx}, {Component: "nats", Recover: testTx}} {
		if err := Validate(o); err != nil {
			t.Fatalf("legal operands rejected: %+v: %v", o, err)
		}
	}
	for _, o := range []Options{{Component: "nats", Status: "bad"}, {Component: "caddy", Status: testTx, Recover: testTx}, {Component: "caddy", Status: testTx, DryRun: true}, {Component: "caddy", Rollback: true, ReloadConfig: true, ConfigSHA256: strings.Repeat("b", 64)}} {
		if err := Validate(o); classOf(err) != 64 {
			t.Fatalf("invalid query/reload accepted: %+v %v", o, err)
		}
	}
	if Selected(Options{}) || !Selected(Options{Status: testTx}) || !Selected(Options{ConfigSHA256: "bad"}) {
		t.Fatal("default whole-host selection changed")
	}
	var calls []proto.InfraCommand
	err := Execute(t.Context(), io.Discard, base, func(_ context.Context, r proto.InfraCommand) (*proto.InfraUpgradeResp, error) {
		calls = append(calls, r)
		if r.Op == "probe" {
			return &proto.InfraUpgradeResp{OK: true, Capability: &proto.InfraCapability{Version: 1, NATS: true}}, nil
		}
		return nil, fmt.Errorf("lost ACK")
	})
	if classOf(err) != 74 || len(calls) != 2 || calls[0].Op != "probe" || calls[1].Op != "start" || !hexValue(calls[1].TxID, 16) || !strings.Contains(err.Error(), calls[1].TxID) || calls[1].RemainingNS >= calls[0].RemainingNS {
		t.Fatalf("lost ACK retried or lost budget/Tx: calls=%+v err=%v", calls, err)
	}
	for _, cap := range []*proto.InfraUpgradeResp{nil, {OK: true}, {OK: true, Capability: &proto.InfraCapability{Version: 1, Caddy: true}}, {Code: "infra_not_enabled"}} {
		n := 0
		err := Execute(t.Context(), io.Discard, base, func(context.Context, proto.InfraCommand) (*proto.InfraUpgradeResp, error) { n++; return cap, nil })
		if err == nil || n != 1 {
			t.Fatalf("missing capability reached Start: n=%d err=%v", n, err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	cancel()
	r := command(base, testTx)
	if err := remaining(ctx, &r); classOf(err) != 75 {
		t.Fatalf("cancelled budget was renewed: %v", err)
	}
	var preview bytes.Buffer
	base.DryRun = true
	n := 0
	if err := Execute(t.Context(), &preview, base, func(_ context.Context, r proto.InfraCommand) (*proto.InfraUpgradeResp, error) {
		n++
		if r.Op != "probe" {
			t.Error("dry-run sent an effect")
		}
		return &proto.InfraUpgradeResp{OK: true, Capability: &proto.InfraCapability{Version: 1, NATS: true}}, nil
	}); err != nil || n != 1 || !strings.Contains(preview.String(), "dry-run") {
		t.Fatalf("dry-run: n=%d err=%v out=%q", n, err, &preview)
	}
}

// origin: hot-update S9 plan §7.1 / TestInfraCLILifecycle
func TestInfraCLILifecycle(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInfraCLI(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra CLI transport x5", g)
	testharness.AssertNoFDLeak(t, "infra CLI transport x5", fd, 0)
}

func exerciseInfraCLI(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "admin.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = conn.Close() }()
		var req adminsock.Request
		if err := json.NewDecoder(conn).Decode(&req); err != nil {
			done <- err
			return
		}
		if req.Infra == nil || req.Op != adminsock.OpInfraStatus || req.Infra.TxID != testTx {
			done <- fmt.Errorf("wrong local command")
			return
		}
		done <- json.NewEncoder(conn).Encode(adminsock.Response{OK: true, Infra: &proto.InfraUpgradeResp{OK: true, Report: report("halted")}})
	}()
	cmd := AdminCommand(func() string { return path }, AdminPorts{Call: CallAdmin, Exit: func(err error) error { return err }})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetContext(t.Context())
	cmd.SetArgs([]string{"status", "--tx", testTx})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if os.Geteuid() != 0 {
		if err := os.Chmod(path, 0000); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		r := command(Options{Status: testTx}, testTx)
		if _, err := CallAdmin(ctx, path, adminsock.Request{Op: adminsock.OpInfraStatus, Infra: &r}); classOf(err) != 77 {
			t.Fatalf("socket UID denial was treated as unavailability: %v", err)
		}
		if err := os.Chmod(path, 0600); err != nil {
			t.Fatal(err)
		}
	}
	var s c.Status
	if err := json.Unmarshal(out.Bytes(), &s); err != nil || s.Phase != c.Halted {
		t.Fatalf("local query marked completed: %s err=%v", &out, err)
	}
	// Cancel only after the peer has consumed the request, so this exercises
	// a blocked read and the joined close callback rather than a failed dial.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = conn.Close() }()
		var req adminsock.Request
		if err := json.NewDecoder(conn).Decode(&req); err != nil {
			done <- err
			return
		}
		cancel()
		var one [1]byte
		_, err = conn.Read(one[:])
		if !errors.Is(err, io.EOF) {
			done <- fmt.Errorf("cancel retained peer socket: %v", err)
			return
		}
		done <- nil
	}()
	bounded, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	r := command(Options{Status: testTx}, testTx)
	if _, err := CallAdmin(bounded, path, adminsock.Request{Op: adminsock.OpInfraStatus, Infra: &r}); classOf(err) != 69 {
		t.Fatalf("cancelled query transport: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §7.1 / T52 / T53
func TestInfraCLISignsSeparateSubjectAndNeverFallsBack(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url, nats.CustomInboxPrefix("_TINBOX.cli"))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
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
	actorKey, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer actorKey.Wipe()
	actor, err := actorKey.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan proto.InfraUpgradeReq, 1)
	sub, err := nc.Subscribe(proto.SubjClusterInfra("node-a"), func(m *nats.Msg) {
		r, err := proto.DecodeInfraUpgradeRequest(m.Data)
		if err == nil {
			err = proto.VerifyInfraUpgradeRequest(r, pub, actor, "node-a", "_TINBOX.cli", m.Reply, time.Now())
		}
		if err != nil {
			_ = m.Respond([]byte(`{"code":"infra_permission_denied"}`))
			return
		}
		seen <- r
		b, _ := json.Marshal(proto.InfraUpgradeResp{OK: true, Report: report("halted")})
		_ = m.Respond(b)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	unavailable := func(f string, a ...any) error { return &ExitError{Class: 69, Err: fmt.Errorf(f, a...)} }
	resp, err := Remote(t.Context(), nc, actor, seed, "node-a", command(Options{Status: testTx}, testTx), unavailable)
	if err != nil || resp == nil || !resp.OK {
		t.Fatalf("signed request failed: %+v %v", resp, err)
	}
	if r := <-seen; r.Request.Op != "status" || r.Request.TxID != testTx || r.Nonce == "" {
		t.Fatalf("wrong signed request: %+v", r)
	}
	// No responder is unavailable; it must not publish a legacy trigger.
	legacy := make(chan struct{}, 1)
	legacySub, err := nc.Subscribe(proto.SubjCtrlClusterUpgrade(actor), func(*nats.Msg) { legacy <- struct{}{} })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = legacySub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := Remote(t.Context(), nc, actor, seed, "absent", command(Options{Status: testTx}, testTx), unavailable); classOf(err) != 69 {
		t.Fatalf("missing owner misclassified: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-legacy:
		t.Fatal("infra fell back to broker reload")
	default:
	}
}

// origin: hot-update S9 plan §3.3 / §7.1 / T52 / M26
func TestInfraCLIRosterAndCapabilityFailClosed(t *testing.T) {
	key, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer key.Wipe()
	seed, err := key.Seed()
	if err != nil {
		t.Fatal(err)
	}
	defer clear(seed)
	pub, err := key.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	roster, err := clusterroster.Build(seed, pub, 1, []proto.RosterBroker{{NodeID: "node-a", Phase: proto.RosterPhaseVoter}, {NodeID: "node-b", Phase: proto.RosterPhaseVoter}}, time.Now().Add(-time.Minute).UTC().Format(time.RFC3339), time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	capable := &proto.InfraCapability{Version: 1, NATS: true, Caddy: true}
	for _, tc := range []struct {
		name          string
		health        []proto.ClusterHealthResp
		manifest      *proto.ClusterManifest
		rejectProbe   bool
		haltSecond    bool
		class, starts int
	}{
		{"complete", []proto.ClusterHealthResp{{NodeID: "node-b", WritableLeaderConfirmed: true, InfraCapability: capable}, {NodeID: "node-a", InfraCapability: capable}}, &proto.ClusterManifest{Roster: roster}, false, false, 0, 2},
		{"absent", []proto.ClusterHealthResp{{NodeID: "node-a", InfraCapability: capable}}, &proto.ClusterManifest{Roster: roster}, false, false, 75, 0},
		{"duplicate", []proto.ClusterHealthResp{{NodeID: "node-a", InfraCapability: capable}, {NodeID: "node-a", InfraCapability: capable}}, &proto.ClusterManifest{Roster: roster}, false, false, 75, 0},
		{"unexpected", []proto.ClusterHealthResp{{NodeID: "node-a", InfraCapability: capable}, {NodeID: "node-c", InfraCapability: capable}}, &proto.ClusterManifest{Roster: roster}, false, false, 75, 0},
		{"missing_roster", []proto.ClusterHealthResp{{NodeID: "node-a", InfraCapability: capable}, {NodeID: "node-b", InfraCapability: capable}}, nil, false, false, 75, 0},
		{"missing_capability", []proto.ClusterHealthResp{{NodeID: "node-a", InfraCapability: capable}, {NodeID: "node-b"}}, &proto.ClusterManifest{Roster: roster}, false, false, 64, 0},
		{"probe_refusal", []proto.ClusterHealthResp{{NodeID: "node-a", InfraCapability: capable}, {NodeID: "node-b", InfraCapability: capable}}, &proto.ClusterManifest{Roster: roster}, true, false, 75, 0},
		{"halt_second", []proto.ClusterHealthResp{{NodeID: "node-b", WritableLeaderConfirmed: true, InfraCapability: capable}, {NodeID: "node-a", InfraCapability: capable}}, &proto.ClusterManifest{Roster: roster}, false, true, 74, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := testharness.StartNATS(t)
			nc, err := nats.Connect(url)
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			var starts atomic.Int32
			order := make(chan string, 4)
			sub, err := nc.Subscribe(proto.SubjClusterPrefix+".infra.*.req", func(m *nats.Msg) {
				req, err := proto.DecodeInfraUpgradeRequest(m.Data)
				if err != nil {
					_ = m.Respond([]byte(`{"code":"infra_config_invalid"}`))
					return
				}
				resp := proto.InfraUpgradeResp{OK: true, Capability: capable}
				if req.Request.Op == "probe" && tc.rejectProbe && req.TargetNode == "node-b" {
					resp = proto.InfraUpgradeResp{Code: "infra_not_ready"}
				}
				if req.Request.Op == "start" {
					starts.Add(1)
					order <- req.TargetNode
					r := report("completed")
					r.TxID = req.Request.TxID
					if tc.haltSecond && req.TargetNode == "node-b" {
						r.Phase, r.Code = "halted", "infra_recovery_required"
					}
					resp = proto.InfraUpgradeResp{OK: true, Report: r}
				}
				body, _ := json.Marshal(resp)
				_ = m.Respond(body)
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			if err := nc.Flush(); err != nil {
				t.Fatal(err)
			}
			home := t.TempDir()
			if _, err := cli.EnsureIdentity(home); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(home, "current_session"), []byte("lab\n"), 0600); err != nil {
				t.Fatal(err)
			}
			seedPath := filepath.Join(home, "account.nk")
			if err := os.WriteFile(seedPath, seed, 0600); err != nil {
				t.Fatal(err)
			}
			cmd := &cobra.Command{}
			cmd.SetContext(t.Context())
			var out bytes.Buffer
			cmd.SetOut(&out)
			err = RunCluster(cmd, Options{Component: "nats", Rollback: true}, home, url, seedPath, ClusterPorts{
				Connect: func(*cobra.Command, string, string, string, *cli.Identity, nats.Option) (*nats.Conn, error) {
					return nats.Connect(url)
				},
				Health: func(*nats.Conn, string) ([]proto.ClusterHealthResp, error) {
					return append([]proto.ClusterHealthResp(nil), tc.health...), nil
				},
				Manifest:    func(context.Context, *nats.Conn, string) *proto.ClusterManifest { return tc.manifest },
				Unavailable: func(f string, a ...any) error { return &ExitError{Class: 69, Err: fmt.Errorf(f, a...)} },
			})
			if classOf(err) != tc.class || int(starts.Load()) != tc.starts {
				t.Fatalf("class=%d starts=%d want=%d/%d err=%v", classOf(err), starts.Load(), tc.class, tc.starts, err)
			}
			if tc.starts > 0 {
				if first := <-order; first != "node-a" {
					t.Fatalf("leader upgraded before follower: %s", first)
				}
			}
			if tc.class != 0 && out.Len() != 0 {
				t.Fatalf("halt leaked earlier transaction identities: %s", &out)
			}
		})
	}
}
