package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/auth"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// origin: hot-update S6 internal review rv-ctl-12 / plan §8.2
func TestLocalReloadUsesComponentBudgetAndKeepsUnknownResult(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLocalReloadCompletion(t)
		exerciseLocalReloadPastAdmission(t)
	}
	testharness.AssertNoGoroutineLeak(t, "local reload completion", beforeG)
	testharness.AssertNoFDLeak(t, "local reload completion", beforeFD, 0)
}

func exerciseLocalReloadPastAdmission(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "admin.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), upgradebudget.RequestLatency*2)
	defer cancel()
	done := make(chan error, 1)
	armed := time.Time{}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				done <- err
				return
			}
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			var req adminsock.Request
			if err := json.NewDecoder(conn).Decode(&req); err != nil {
				_ = conn.Close()
				done <- err
				return
			}
			status := &proto.BrokerImageStatus{Generation: 7, RunningSHA256: rollOldSHA, Phase: "serving", Mode: "single"}
			response := adminsock.Response{OK: true, Runtime: &adminsock.RuntimeReport{BrokerImage: status}}
			completed := false
			if req.Op == adminsock.OpBrokerUpgradeReload {
				if !armed.IsZero() {
					_ = conn.Close()
					done <- errors.New("accepted reload replayed")
					return
				}
				armed = time.Now()
				response.NextGeneration = 8
				response.BrokerImage = &proto.BrokerImageStatus{CutID: "original-cut", Phase: "preparing"}
			} else if !armed.IsZero() {
				status.CutID, status.Generation, status.RunningSHA256 = "original-cut", 8, rollNewSHA
				status.Phase = "control_ready"
				if time.Since(armed) >= upgradebudget.RequestLatency+100*time.Millisecond {
					status.Phase, completed = "serving", true
				}
			}
			err = json.NewEncoder(conn).Encode(response)
			_ = conn.Close()
			if err != nil || completed {
				done <- err
				return
			}
		}
	}()
	status, reloadErr := ReloadLocalBroker(ctx, path, "v2", rollNewSHA)
	_ = listener.Close() // Wake the server on a deadline mutant before joining it.
	serverErr := <-done
	if reloadErr != nil || status == nil || status.Phase != "serving" || serverErr != nil {
		t.Fatalf("component completion inherited the five-second RPC cap: status=%+v err=%v server=%v", status, reloadErr, serverErr)
	}
}

func exerciseLocalReloadCompletion(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "admin.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	// The accepted cut returns an armed receipt and then cancels the component
	// while a runtime poll is pending. No second reload is allowed.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	var reloads int
	go func() {
		for index := 0; index < 3; index++ {
			conn, err := listener.Accept()
			if err != nil {
				done <- err
				return
			}
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			var req adminsock.Request
			if err := json.NewDecoder(conn).Decode(&req); err != nil {
				_ = conn.Close()
				done <- err
				return
			}
			response := adminsock.Response{OK: true}
			switch index {
			case 0:
				response.Runtime = &adminsock.RuntimeReport{BrokerImage: &proto.BrokerImageStatus{Generation: 7, RunningSHA256: rollOldSHA, Phase: "serving", Mode: "single"}}
			case 1:
				if req.Op != adminsock.OpBrokerUpgradeReload {
					_ = conn.Close()
					done <- errors.New("reload was not armed")
					return
				}
				reloads++
				response.NextGeneration = 8
				response.BrokerImage = &proto.BrokerImageStatus{CutID: "original-cut", Generation: 7, Phase: "preparing", Mode: "single"}
			case 2:
				if req.Op != adminsock.OpRuntime {
					_ = conn.Close()
					done <- errors.New("unsafe reload replayed")
					return
				}
				cancel()
			}
			err = json.NewEncoder(conn).Encode(response)
			_ = conn.Close()
			if err != nil && index != 2 {
				done <- err
				return
			}
		}
		done <- nil
	}()
	_, err = ReloadLocalBroker(ctx, path, "v2", rollNewSHA)
	var unknown *BrokerReloadUnknownError
	if !errors.As(err, &unknown) {
		t.Errorf("armed but unconfirmed reload lost its unknown-result class: %v", err)
	}
	if serverErr := <-done; serverErr != nil || reloads != 1 {
		t.Fatalf("reload owner did not join: reloads=%d err=%v", reloads, serverErr)
	}
}

var rollOldSHA = strings.Repeat("a", 64)
var rollNewSHA = strings.Repeat("b", 64)

func rollHealth(leader string) []proto.ClusterHealthResp {
	var hs []proto.ClusterHealthResp
	for _, id := range []string{"a", "b", "c"} {
		hs = append(hs, proto.ClusterHealthResp{NodeID: id, IsVoter: true, LeaderID: leader,
			WritableLeaderConfirmed: id == leader, AppliedIndex: 10, ReleaseVersion: "v2",
			BrokerImage: &proto.BrokerImageStatus{RunningSHA256: rollOldSHA, Generation: 7, Phase: "serving", Mode: "quorum"}})
	}
	return hs
}

func rollConfig() ClusterRollStep {
	return ClusterRollStep{Step: clusterupgrade.Step{Kind: clusterupgrade.StepUpgrade, NodeID: "b", TransferTo: "c"},
		Scope: clusterupgrade.ScopeBrokerDaemon, Voters: []string{"a", "b", "c"}, ToVersion: "v2", ExpectSHA256: rollNewSHA,
		Holder: "holder-b", CutID: "cut-b", PollInterval: time.Millisecond}
}

// origin: hot-update S6 plan §5 / T38
func TestTransferLeaderEveryStep(t *testing.T) {
	// The plan says c, but b became leader after planning. The live caught-up
	// survivor is a. A static TransferTo would transfer to the wrong voter.
	leader, reloaded, reads := "b", false, 0
	var ops []string
	io := ClusterRollIO{
		Health: func(context.Context) ([]proto.ClusterHealthResp, error) {
			reads++
			hs := rollHealth(leader)
			// A complete census at the transfer edge can precede VerifyLeader.
			// That temporary observation must consume this same poll budget.
			// origin: hot-update S6 plan §5 / T38 / rv-cluster-10
			if reads == 2 {
				for i := range hs {
					hs[i].WritableLeaderConfirmed = false
				}
			}
			// c was caught up when the plan was made, but has fallen behind.
			// Only a is now a safe destination, so the stale hint is unsafe.
			hs[2].AppliedIndex = 1
			if reloaded {
				hs[1].BrokerImage = &proto.BrokerImageStatus{RunningSHA256: rollNewSHA, Generation: 8, CutID: "cut-b", Mode: "quorum", Phase: "serving"}
			}
			return hs, nil
		},
		Trigger: func(_ context.Context, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
			if req.Holder != "holder-b" || req.Scope != "broker-daemon" {
				t.Fatalf("lost signed holder/scope: %+v", req)
			}
			ops = append(ops, req.Op)
			if req.Op == "maintenance" {
				return rollMaintenanceResponse(t, req), nil
			}
			if req.Op == "transfer-leader" {
				if req.TargetNode != "b" || req.TransferTo != "a" {
					t.Fatalf("static transfer target used: %+v", req)
				}
				leader = "a"
				return &proto.ClusterUpgradeResp{OK: true}, nil
			}
			if req.Op != "reload" || leader == "b" || req.CutID != "cut-b" {
				t.Fatalf("unsafe cut before fresh transfer: %+v leader=%s", req, leader)
			}
			reloaded = true
			return &proto.ClusterUpgradeResp{OK: true, Reloading: true, NextGeneration: 8}, nil
		},
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := RollBrokerStep(ctx, io, rollConfig()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(ops, ",") != "transfer-leader,maintenance,reload,maintenance" || reads < 4 {
		t.Fatalf("unverified transfer: ops=%v reads=%d", ops, reads)
	}

	for _, defect := range []string{"membership", "two-leaders", "uncaught", "stale-target", "health-error", "role-ABA", "transfer-unverified"} {
		t.Run(defect, func(t *testing.T) {
			hs := rollHealth("b")
			switch defect {
			case "membership":
				hs[2].IsVoter = false
			case "two-leaders":
				hs[0].WritableLeaderConfirmed = true
			case "uncaught":
				hs[0].AppliedIndex = 1
				hs[2].AppliedIndex = 1
			case "stale-target":
				hs[1].LeaderContactStale = true
			}
			cuts, reads := 0, 0
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			io := ClusterRollIO{
				Health: func(context.Context) ([]proto.ClusterHealthResp, error) {
					reads++
					if (defect == "transfer-unverified" || defect == "uncaught") && reads > 1 {
						cancel()
					}
					if defect == "health-error" {
						return nil, errors.New("read failed")
					}
					return hs, nil
				},
				Trigger: func(_ context.Context, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
					if req.Op == "maintenance" && req.TargetNode != "" {
						return rollMaintenanceResponse(t, req), nil
					}
					if req.Op == "transfer-leader" {
						if defect != "transfer-unverified" {
							hs = rollHealth("a")
						}
						return &proto.ClusterUpgradeResp{OK: true}, nil
					}
					cuts++
					// The server's continuous observer rejects an ABA that no
					// pair of client reads can detect. A refusal cannot be retried.
					return &proto.ClusterUpgradeResp{Code: adminsock.CodeIsLeader, Error: "role edge changed"}, nil
				},
			}
			if err := RollBrokerStep(ctx, io, rollConfig()); err == nil {
				t.Fatal("unsafe topology admitted")
			}
			if (defect == "role-ABA" && cuts != 1) || (defect != "role-ABA" && cuts != 0) {
				t.Fatalf("unsafe cut count=%d", cuts)
			}
		})
	}
}

// origin: hot-update S6 plan §5 / T45
func TestBrokerRollRequiresRunningImage(t *testing.T) {
	for _, defect := range []string{"sha", "generation", "future-generation", "cut", "phase", "mode", "missing", "lag", "already-version"} {
		t.Run(defect, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reads, reloads := 0, 0
			io := ClusterRollIO{
				Health: func(context.Context) ([]proto.ClusterHealthResp, error) {
					reads++
					hs := rollHealth("a")
					if reads > 1 {
						s := &proto.BrokerImageStatus{RunningSHA256: rollNewSHA, Generation: 8, CutID: "cut-b", Phase: "serving", Mode: "quorum"}
						switch defect {
						case "sha":
							s.RunningSHA256 = rollOldSHA
						case "generation":
							s.Generation = 7
						case "future-generation":
							s.Generation = 9
						case "cut":
							s.CutID = "other-cut"
						case "phase":
							s.Phase = "control_ready"
						case "mode":
							s.Mode = "unknown"
						case "missing":
							s = nil
						case "lag":
							hs[0].AppliedIndex = 1000
						case "already-version":
							s.RunningSHA256 = rollOldSHA
							s.Generation = 7
						}
						hs[1].BrokerImage = s
						cancel() // deterministic failure; success before observing cancellation is a false completion
					}
					return hs, nil
				},
				Trigger: func(_ context.Context, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
					if req.Op == "maintenance" && req.TargetNode != "" {
						return rollMaintenanceResponse(t, req), nil
					}
					reloads++
					if defect == "already-version" {
						return &proto.ClusterUpgradeResp{OK: true, AlreadyAtVersion: true}, nil
					}
					return &proto.ClusterUpgradeResp{OK: true, Reloading: true, NextGeneration: 8}, nil
				},
			}
			if err := RollBrokerStep(ctx, io, rollConfig()); err == nil {
				t.Fatal("same version/armed reply falsely completed the roll")
			}
			if reloads != 1 || reads != 2 {
				t.Fatalf("unexpected retry: reloads=%d reads=%d", reloads, reads)
			}
		})
	}
	// A sent reload with a lost reply can complete by query, with no second send.
	reads, reloads := 0, 0
	io := ClusterRollIO{
		Health: func(context.Context) ([]proto.ClusterHealthResp, error) {
			reads++
			hs := rollHealth("a")
			if reads > 1 {
				hs[1].BrokerImage = &proto.BrokerImageStatus{RunningSHA256: rollNewSHA, Generation: 8, CutID: "cut-b", Phase: "serving", Mode: "quorum"}
			}
			return hs, nil
		},
		Trigger: func(_ context.Context, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
			if req.Op == "maintenance" && req.TargetNode != "" {
				return rollMaintenanceResponse(t, req), nil
			}
			reloads++
			return nil, errors.New("reply lost")
		},
	}
	if err := RollBrokerStep(t.Context(), io, rollConfig()); err != nil || reloads != 1 {
		t.Fatalf("unknown result query: err=%v reloads=%d", err, reloads)
	}
}

// origin: hot-update S6 plan §5 / T45
func TestBrokerRollPlanUsesRunningSHA(t *testing.T) {
	nodes := []clusterupgrade.Node{{ID: "b", Voter: true, BrokerVer: "v2", AgentVer: "v1", Agent: clusterupgrade.AgentPresent}}
	hs := rollHealth("a")
	p := PlanClusterRoll(nodes, "v2", clusterupgrade.ScopeBrokerDaemon, rollNewSHA, hs)
	if p.Upgrades() != 1 || p.Steps[0].BrokerOnly {
		t.Fatalf("same version or forged agent absence skipped candidate: %+v", p)
	}
	hs[1].BrokerImage.RunningSHA256 = rollNewSHA
	if p := PlanClusterRoll(nodes, "v2", clusterupgrade.ScopeBrokerDaemon, rollNewSHA, hs); p.Upgrades() != 0 {
		t.Fatalf("running candidate not skipped: %+v", p)
	}
	if p := PlanClusterRoll(nodes, "v2", clusterupgrade.ScopeWholeHost, rollNewSHA, hs); p.Upgrades() != 1 {
		t.Fatal("whole-host skipped stale agent")
	}
}

// origin: hot-update S6 plan §11 / T54 / P24
func TestBrokerRollLeakCycles(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRollLeaderChanged(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker roll cancellation", beforeG)
	testharness.AssertNoFDLeak(t, "broker roll cancellation", beforeFD, 0)
}

func exerciseRollLeaderChanged(t *testing.T) {
	t.Helper()
	// Real Unix IO is blocked in read when cancellation closes the client.
	// Waiting for the server's EOF makes a forgotten close visible in each round.
	path := filepath.Join(t.TempDir(), "admin.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetDeadline(time.Now().Add(time.Second))
		var req adminsock.Request
		if err := json.NewDecoder(c).Decode(&req); err != nil {
			done <- err
			return
		}
		cancel()
		b := make([]byte, 1)
		_, err = c.Read(b)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			done <- errors.New("client cancellation leaked its socket")
			return
		}
		done <- nil
	}()
	leader := "b"
	io := ClusterRollIO{
		Health: func(context.Context) ([]proto.ClusterHealthResp, error) { return rollHealth(leader), nil },
		Trigger: func(rctx context.Context, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
			if req.Op == "maintenance" {
				return rollMaintenanceResponse(t, req), nil
			}
			if req.Op == "transfer-leader" {
				leader = req.TransferTo
				return &proto.ClusterUpgradeResp{OK: true}, nil
			}
			_, err := CallBrokerAdmin(rctx, path, adminsock.Request{Op: adminsock.OpRuntime})
			return nil, err
		},
	}
	if err := RollBrokerStep(ctx, io, rollConfig()); err == nil || !strings.Contains(err.Error(), "control_result_unknown") {
		t.Fatalf("cancelled unknown roll result: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Exercise the fresh leader selection itself in the same resource cycle.
	hs := rollHealth("b")
	l, _, _, err := rollTopology(hs, "b", []string{"c", "b", "a"})
	if err != nil || l.NodeID != "b" {
		t.Fatalf("changed leader: %s %v", l.NodeID, err)
	}
}

func rollMaintenanceResponse(t *testing.T, req *proto.ClusterUpgradeReq) *proto.ClusterUpgradeResp {
	t.Helper()
	if req.Holder != "holder-b" || req.CutID != "cut-b" || req.Scope != "broker-daemon" {
		t.Fatal("maintenance lost cut authority")
	}
	if req.Maintenance != nil {
		if req.Maintenance.Phase != brokermaint.MaintEnded {
			t.Fatal("maintenance did not end")
		}
		return &proto.ClusterUpgradeResp{OK: true, Maintenance: req.Maintenance}
	}
	if req.TransferTo != "b" || req.TargetNode == req.TransferTo {
		t.Fatal("maintenance did not use the fresh surviving leader")
	}
	return &proto.ClusterUpgradeResp{OK: true, Maintenance: &brokermaint.Record{CutID: req.CutID, Holder: req.Holder, Home: req.TransferTo, Generation: 7, Phase: brokermaint.MaintActive}}
}

// Positive pre-S6 health is the legacy proof, and a real old-bytes verifier
// must accept every signed request while the roll observes the new version.
// origin: hot-update S6 internal review rv-cluster-8
func TestBrokerRollUsesExplicitLegacySignedBytes(t *testing.T) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := auth.PublicKeyFromSeed(seed)
	hs := rollHealth("a")
	for i := range hs {
		hs[i].SchemaVersion = 7
		hs[i].BrokerImage = nil
		hs[i].ReleaseVersion = "v1"
	}
	reloads := 0
	io := ClusterRollIO{
		Health: func(context.Context) ([]proto.ClusterHealthResp, error) { return hs, nil },
		Trigger: func(_ context.Context, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
			if req.Holder != "" || req.Scope != "" || req.CutID != "" || req.Maintenance != nil {
				t.Fatal("legacy roll signed an S6 extension")
			}
			req.IssuedAt = "2026-10-12T00:00:00Z"
			old := []byte(req.Op + "\n" + req.TargetNode + "\n" + req.TransferTo + "\n" + req.ToVersion + "\n" + req.ExpectSHA256 + "\n" + req.SID + "\n" + req.IssuedAt)
			sig, err := auth.SignWithSeed(seed, proto.CanonicalUpgradeReqBytes(req))
			if err != nil || auth.VerifySignature(pub, old, sig) != nil {
				t.Fatal("old verifier rejected new ctl bytes")
			}
			if req.Op != "reload" {
				t.Fatalf("legacy roll entered new protocol: %s", req.Op)
			}
			reloads++
			hs[1].ReleaseVersion = "v2"
			return &proto.ClusterUpgradeResp{OK: true, Reloading: true}, nil
		},
	}
	cfg := rollConfig()
	cfg.Holder = ""
	if err := RollBrokerStep(t.Context(), io, cfg); err != nil || reloads != 1 {
		t.Fatalf("legacy roll=%d %v", reloads, err)
	}
	hs[0].SchemaVersion = 8
	hs[1].SchemaVersion = 8
	hs[2].SchemaVersion = 8
	if LegacyBrokerRoll(hs) {
		t.Fatal("missing current image status was treated as positive old-version proof")
	}
}

// Missing installed-observer authority must stop before the unsafe reload.
// origin: hot-update S6 internal review rv-cluster-3
func TestBrokerRollRequiresMaintenanceBeforeReload(t *testing.T) {
	for _, defect := range []string{"unsupported", "missing-record", "wrong-holder", "wrong-cut", "wrong-home", "wrong-generation", "prepared"} {
		t.Run(defect, func(t *testing.T) {
			reloads := 0
			io := ClusterRollIO{
				Health: func(context.Context) ([]proto.ClusterHealthResp, error) { return rollHealth("a"), nil },
				Trigger: func(_ context.Context, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
					if req.Op == "reload" {
						reloads++
						return nil, errors.New("unsafe reload reached")
					}
					response := rollMaintenanceResponse(t, req)
					switch defect {
					case "unsupported":
						response.OK = false
					case "missing-record":
						response.Maintenance = nil
					case "wrong-holder":
						response.Maintenance.Holder = "replacement"
					case "wrong-cut":
						response.Maintenance.CutID = "replacement"
					case "wrong-home":
						response.Maintenance.Home = "c"
					case "wrong-generation":
						response.Maintenance.Generation++
					case "prepared":
						response.Maintenance.Phase = brokermaint.MaintPrepared
					}
					return response, nil
				},
			}
			if err := RollBrokerStep(t.Context(), io, rollConfig()); err == nil || reloads != 0 {
				t.Fatalf("unproved maintenance reached reload: %d %v", reloads, err)
			}
		})
	}
}

// A WAN voter replying beyond the former 500ms window still belongs in this
// bounded RPC. All callbacks and timers are joined in every exercise round.
// origin: hot-update S6 internal review rv-cluster-10
func TestBrokerRollHealthWaitsForExpectedVoters(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseDelayedBrokerHealth(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker health delayed reply x5", beforeG)
	testharness.AssertNoFDLeak(t, "broker health delayed reply x5", beforeFD, 0)
}

func exerciseDelayedBrokerHealth(t *testing.T) {
	t.Helper()
	server, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1})
	if err != nil {
		t.Fatal(err)
	}
	server.Start()
	t.Cleanup(func() { server.Shutdown(); server.WaitForShutdown() })
	if !server.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS not ready")
	}
	nc, err := nats.Connect(server.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	slowDone := make(chan struct{})
	var timer *time.Timer
	var timerMu sync.Mutex
	sub, err := nc.Subscribe(proto.SubjCtrlClusterHealth("actor"), func(msg *nats.Msg) {
		for _, id := range []string{"a", "b"} {
			body, _ := json.Marshal(proto.ClusterHealthResp{NodeID: id, IsVoter: true})
			_ = msg.Respond(body)
		}
		timerMu.Lock()
		timer = time.AfterFunc(750*time.Millisecond, func() {
			defer close(slowDone)
			body, _ := json.Marshal(proto.ClusterHealthResp{NodeID: "c", IsVoter: true})
			_ = msg.Respond(body)
		})
		timerMu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sub.Unsubscribe()
		timerMu.Lock()
		pending := timer
		timerMu.Unlock()
		if pending != nil && !pending.Stop() {
			<-slowDone
		}
	})
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	hs, err := QueryBrokerRollHealthForVoters(ctx, nc, "actor", []string{"a", "b", "c"})
	if err != nil || len(hs) != 3 {
		t.Fatalf("slow configured voter disappeared: %d %v", len(hs), err)
	}
	select {
	case <-slowDone:
	case <-ctx.Done():
		t.Fatal("health callback did not join")
	}
}
