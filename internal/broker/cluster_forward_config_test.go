package broker

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/clusterharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §2.1 / T05 / T39
func TestTransferClusterConfigUsesRetiredCoordinatorProof(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("leader_handoff", func(t *testing.T) { exerciseTransferConfigHandoff(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "leader config handoff", beforeG)
	testharness.AssertNoFDLeak(t, "leader config handoff", beforeFD, 0)
}
func exerciseTransferConfigHandoff(t *testing.T) {
	t.Helper()
	c := newTransferRouteCluster(t)
	// Routing uses three real Raft owners and routed NATS ingresses. Config IO
	// uses real file-store JS; this test does not claim R3 JS data replication.
	s := natstest.RunServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: true, StoreDir: t.TempDir()})
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	// ctx-root: test owns the embedded cluster and its bounded configuration IO.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	probes := make([]clusterharness.LeaderProbe, 3)
	for i, n := range c.nodes {
		probes[i] = n
	}
	var old int
	var coordinator xfercontract.BucketPort
	create := func(index int, generation uint64, previous *xfercontract.ConfigWriterState) xfercontract.BucketPort {
		identity := xfercontract.ConfigWriterProof{BrokerID: c.origins[index].BrokerID, InstanceID: c.origins[index].InstanceID, BootID: "boot", Nonce: "config-nonce", Generation: generation}
		authority, err := xferbucket.NewAuthority(identity, previous)
		if err != nil {
			t.Fatal(err)
		}
		local, err := xferbucket.New(xferbucket.Config{Base: xfercontract.Base{Clock: func() (string, int64, error) { now, err := controlbudget.Now(); return "boot", now, err }}, JS: js, Authority: authority})
		if err != nil {
			t.Fatal(err)
		}
		return local
	}
	_, err = clusterharness.WithLeader(t, probes, 5*time.Second, func(leader int) error {
		old = leader
		coordinator = create(leader, 1, nil)
		if err := InstallTransferClusterForward(c.brokers[leader], c.conns[leader], TransferClusterForwardConfig{Node: c.nodes[leader], Origin: c.origins[leader], Buckets: coordinator}); err != nil {
			return err
		}
		first := c.brokers[leader].cl.transferRPC
		if first == nil {
			t.Fatal("installed RPC callbacks have no retained join owner")
		}
		if err := InstallTransferClusterForward(c.brokers[leader], c.conns[leader], TransferClusterForwardConfig{Node: c.nodes[leader], Origin: c.origins[leader], Buckets: coordinator}); err == nil || c.brokers[leader].cl.transferRPC != first {
			t.Fatal("second installation displaced a live RPC owner")
		}
		if err := c.conns[leader].FlushTimeout(time.Second); err != nil {
			return err
		}
		follower := (leader + 1) % 3
		c.brokers[follower].cl.forwarder = NewForwarderWithLimit(c.conns[follower], time.Second, 0)
		c.brokers[follower].transferRuntime.Store(&BrokerTransfers{Buckets: transferRouteRefusingBucket{err: errors.New("follower local writer entered")}})
		router := brokerTransferBuckets(c.brokers[follower])
		seal, err := router.Apply(xfercontract.ConfigPatch{Bucket: "xfer-routed"}, transferRouteDeadline(t, time.Second))
		if err != nil {
			return err
		}
		if seal.Proof.BrokerID != c.origins[leader].BrokerID || seal.Duplicates < 2228*time.Second {
			t.Fatalf("forwarded configuration has no original writer proof: %+v", seal)
		}
		// The legacy JS-only signature must not bypass ownership when no binding
		// was supplied, including its replica-raise branch.
		_, unboundErr := XferReplicaState(ctx, js, "routed", 2)
		var unbound *xferbucket.Refusal
		if !errors.As(unboundErr, &unbound) || unbound.Code != xfercontract.CodeOwnerRecovering {
			t.Fatalf("JS-only replica writer bypassed owner: %v", unboundErr)
		}
		stream, err := js.Stream(ctx, seal.Stream)
		if err != nil {
			return err
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return err
		}
		info.Config.Duplicates += time.Minute
		// Legacy full-config callers send only intent through this same router.
		_, err = xferbucket.Bind(js, router).UpdateStream(ctx, info.Config)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := coordinator.Freeze(transferRouteDeadline(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.nodes[old].TransferLeadership(); err != nil {
		t.Fatal(err)
	}
	_, err = clusterharness.WithLeader(t, probes, 5*time.Second, func(leader int) error {
		if leader == old {
			return errors.New("leadership did not leave original writer")
		}
		successor := create(leader, 2, &state)
		if err := InstallTransferClusterForward(c.brokers[leader], c.conns[leader], TransferClusterForwardConfig{Node: c.nodes[leader], Origin: c.origins[leader], Buckets: successor}); err != nil {
			return err
		}
		if err := c.conns[leader].FlushTimeout(time.Second); err != nil {
			return err
		}
		fwd := NewForwarderWithLimit(c.conns[old], time.Second, 0)
		seal, err := ForwardTransferBucketConfig(fwd, xfercontract.ConfigPatch{Bucket: "xfer-routed"}, transferRouteDeadline(t, time.Second))
		if err != nil {
			return err
		}
		if seal.Proof.Generation != 2 || seal.Proof.BrokerID != c.origins[leader].BrokerID {
			t.Fatalf("successor lost handoff proof: %+v", seal)
		}
		if _, err := coordinator.Apply(xfercontract.ConfigPatch{Bucket: "xfer-routed"}, transferRouteDeadline(t, time.Second)); err == nil {
			t.Fatal("retired leader wrote configuration")
		}
		if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-short", MaxBytes: 1 << 30, TTL: time.Minute}); err != nil {
			return err
		}
		_, err = ForwardTransferBucketConfig(fwd, xfercontract.ConfigPatch{Bucket: "xfer-short"}, transferRouteDeadline(t, time.Second))
		var refusal *ForwardBusinessError
		if !errors.As(err, &refusal) || refusal.Kind != xfercontract.CodeStorageHorizon || !strings.Contains(refusal.Msg, "Duplicates") || !strings.Contains(refusal.Msg, "MaxAge") {
			t.Fatalf("real coordinator refusal lost on bus: %v", err)
		}
		return successor.Stop(transferRouteDeadline(t, time.Second))
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range c.brokers {
		if b.cl.transferRPC != nil {
			if err := StopTransferClusterForward(b.cl.transferRPC, transferRouteDeadline(t, time.Second)); err != nil {
				t.Error(err)
			}
		}
	}
}

// origin: hot-update S7 plan §2.1 / T05 / M14
func TestTransferLegacyConfigWritersRequireBoundCoordinator(t *testing.T) {
	source, err := os.ReadFile("transfer.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "transfer.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	guarded := 0
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (sel.Sel.Name != "UpdateStream" && sel.Sel.Name != "CreateObjectStore" && sel.Sel.Name != "UpdateObjectStore" && sel.Sel.Name != "Apply") {
			return true
		}
		receiver, ok := sel.X.(*ast.CallExpr)
		if !ok {
			t.Errorf("legacy config mutation bypasses coordinator: %s", sel.Sel.Name)
			return true
		}
		allowed := false
		if name, ok := receiver.Fun.(*ast.Ident); ok {
			allowed = name.Name == "brokerTransferConfigJS" || name.Name == "brokerTransferBuckets"
		}
		if guard, ok := receiver.Fun.(*ast.SelectorExpr); ok {
			if pkg, ok := guard.X.(*ast.Ident); ok {
				allowed = pkg.Name == "xferbucket" && guard.Sel.Name == "Guard"
			}
		}
		if !allowed {
			t.Errorf("legacy configuration mutation has no bound owner: %s", sel.Sel.Name)
		}
		guarded++
		return true
	})
	if guarded != 4 {
		t.Fatalf("configuration writer census changed: got %d, want 4", guarded)
	}
	publisher, err := os.ReadFile("clusterwrite.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(publisher), "XferReplicaState(ctx, brokerTransferConfigJS(b), sid, target)") {
		t.Fatal("replica publisher did not receive the shared coordinator")
	}
}
