//go:build linux

package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// origin: S6 internal review rv-final F2
func TestClusterControlResumePreservesOneUpgradeResponder(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseClusterControlResumePreservesOneUpgradeResponder(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestClusterControlResumePreservesOneUpgradeResponder x5", g)
	testharness.AssertNoFDLeak(t, "TestClusterControlResumePreservesOneUpgradeResponder x5", fd, 0)
}

func exerciseClusterControlResumePreservesOneUpgradeResponder(t *testing.T) {
	t.Helper()
	b, _ := newClusterProxyBroker(t, "resume-session", "owner")
	nc, err := nats.Connect(startNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	b.nc.Store(nc)
	c := newBrokerImageControl(b)
	b.imageControl = c
	defer func() {
		c.cancel()
		if b.cl.cancel != nil {
			b.cl.cancel()
		}
		d, _ := brokerCutDeadline()
		if err := JoinBrokerStoreLoops(b, d); err != nil {
			t.Error(err)
		}
		for _, s := range b.cl.subs {
			_ = s.Unsubscribe()
		}
	}()
	if err := b.wireClusterLate(c.root, nc); err != nil {
		t.Fatal(err)
	}
	d, err := brokerCutDeadline()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Fence(d); err != nil {
		t.Fatal(err)
	}
	if err := c.JoinStoreUsers(d); err != nil {
		t.Fatal(err)
	}
	// A pre-close Thaw keeps this same Node/NC, renews the root in StartAuth,
	// and invokes wireClusterLate in StartBusiness.
	// ctx-root: replacement control owner after a reversible cut.
	c.root, c.cancel = context.WithCancel(context.Background())
	c.fenced = false
	c.joins = nil
	if err := b.wireClusterLate(c.root, nc); err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, s := range b.cl.subs {
		if s.IsValid() && s.Subject == proto.SubjCtrlClusterUpgradeWildcard {
			count++
		}
	}
	if count != 1 {
		t.Errorf("after reversible cut there are %d live upgrade responders, want 1", count)
	}
	if err := c.Fence(d); err != nil {
		t.Errorf("next cut refuses stale retired subscriptions: %v", err)
	}
}

// origin: S6 internal review rv-final F2
func TestClusterControlRepeatedLegacyRefusalThawsOneOwner(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseClusterRepeatedLegacyRefusal(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "repeated reversible control cuts x5", g)
	testharness.AssertNoFDLeak(t, "repeated reversible control cuts x5", fd, 0)
}

func exerciseClusterRepeatedLegacyRefusal(t *testing.T) {
	t.Helper()
	b, c, store := newJoinedControlFixture(t)
	n, nc := b.cl.node, b.nc.Load()
	base := 0
	for cutN := 0; cutN < 2; cutN++ {
		d, _ := brokerCutDeadline()
		legacy := errors.New("legacy_session_busy")
		cut, err := brokerimage.Freeze(brokerimage.Config{Store: store, Control: c, Mode: brokerimage.ModeSoleVoter, Deadline: d, Server: &tunnel.Server{}, FreezeServer: func(*tunnel.Server, controlbudget.Deadline) (tunnel.ServerFrozen, error) { return nil, legacy }})
		if cut != nil || !errors.Is(err, legacy) {
			t.Fatalf("real Thaw failed: cut=%v err=%v", cut, err)
		}
		if err := n.ApplyMetaSet(fmt.Sprint("t:resume-", cutN), "same-writer"); err != nil {
			t.Fatal(err)
		}
		counts := map[string]int{}
		for _, sub := range b.cl.subs {
			if !sub.IsValid() {
				t.Fatal("retired subscription retained")
			}
			counts[sub.Subject]++
		}
		for subject, count := range counts {
			if count != 1 {
				t.Fatalf("subject %s has %d owners", subject, count)
			}
		}
		if counts[proto.SubjCtrlClusterUpgradeWildcard] != 1 {
			t.Fatal("upgrade reply owner missing")
		}
		if cutN == 0 {
			base = len(b.cl.subs)
		} else if len(b.cl.subs) != base {
			t.Fatalf("control resources grew: %d to %d", base, len(b.cl.subs))
		}
		inbox := nc.NewInbox()
		responses, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatal(err)
		}
		request, _ := json.Marshal(proto.ClusterUpgradeReq{Op: "reload", TargetNode: n.SelfID()})
		if err := nc.PublishRequest(proto.SubjCtrlClusterUpgrade(n.SelfID()), inbox, request); err != nil {
			t.Fatal(err)
		}
		if _, err := responses.NextMsg(time.Second); err != nil {
			t.Fatal(err)
		}
		if _, err := responses.NextMsg(100 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("one request produced multiple replies: %v", err)
		}
		_ = responses.Unsubscribe()
	}
}

func newJoinedControlFixture(t *testing.T) (*Broker, *brokerImageControl, brokerimage.StoreOwner) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")
	_, tr := raft.NewInmemTransport("resume-owner")
	n, err := cluster.New(cluster.Config{LocalID: "resume-owner", DataDir: dir, DBPath: path, Transport: tr, Logger: silentLogger(), ApplyTimeout: 5 * time.Second, ImageRegistry: storage.DefaultDriverRegistry(), ImageStamp: func(db *sql.DB) (storage.StoreStamp, error) {
		id, err := storage.CanonicalStoreIdentity(path, dir)
		if err != nil {
			return storage.StoreStamp{}, err
		}
		return storage.ReadStoreStamp(db, id)
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	if err := n.WaitForLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	lock, err := cluster.AcquireImageDataDirLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Shutdown(); _ = lock.Release() })
	store, err := brokerimage.NewClusterStore(n, lock, cluster.ProductionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	js, nc := newXferTestJS(t)
	if err := jsstream.EnsureEventsStream(t.Context(), js, 1); err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{DB: n.DB(), NATSURL: nc.ConnectedUrl(), Logger: silentLogger()})
	if err != nil {
		t.Fatal(err)
	}
	b.cfg.DB = n.RODB()
	b.cl, b.clusterMode, b.selfID = &clusterRuntime{node: n}, true, n.SelfID()
	b.nc.Store(nc)
	setBrokerJS(b, js)
	c := newBrokerImageControl(b)
	c.store = store
	b.imageControl = c
	t.Cleanup(func() { unbindBrokerImageHealth(b) })
	t.Cleanup(func() { d, _ := brokerCutDeadline(); _ = c.Fence(d); _ = c.JoinStoreUsers(d); _ = c.StopTransport(d) })
	if err := b.wireClusterLate(c.root, nc); err != nil {
		t.Fatal(err)
	}
	return b, c, store
}

// origin: S6 internal review rv-final F1
func TestBrokerFinalReloadGuardRefusalThawsOriginalOwner(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerFinalReloadGuard(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "final role and lease guard x5", g)
	testharness.AssertNoFDLeak(t, "final role and lease guard x5", fd, 0)
}

func exerciseBrokerFinalReloadGuard(t *testing.T) {
	t.Helper()
	for _, drift := range []string{"membership", "lease"} {
		t.Run(drift, func(t *testing.T) {
			b, c, store := newJoinedControlFixture(t)
			n, nc, writer := b.cl.node, b.nc.Load(), b.cl.node.DB()
			d, _ := brokerCutDeadline()
			if err := captureBrokerReloadAdmission(b, adminsock.Request{}, d); err != nil {
				t.Fatal(err)
			}
			slot, err := brokerimage.NewRuntimeSlot(func(brokerimage.StoreOwner, controlbudget.Deadline) error { return nil }, func(tunnel.ServerImported, controlbudget.Deadline) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			r := &BrokerImageRun{Slot: slot, Capture: func(_ *Broker, d controlbudget.Deadline) (brokerimage.Config, error) {
				return brokerimage.Config{Mode: brokerimage.ModeSoleVoter, Store: store, Control: c, Deadline: d}, nil
			}, Freeze: brokerimage.Freeze, Preflight: func(*Broker, controlbudget.Deadline) error { return nil }, PrepareNodeShutdown: cluster.PrepareImageShutdown, InstallCurrent: func(*Broker) error { return nil }, Recover: func(*Broker, brokerimage.Cut, storage.StoreSeal, controlbudget.Deadline) error {
				t.Error("pre-close refusal entered closed recovery")
				return brokerimage.ErrStoreClose
			}, DrainArmed: func(controlbudget.Deadline) error { t.Fatal("unexpected armed cut"); return nil }, WithExecLatch: func(fn func() error) error { return fn() }, CandidatePath: "/never-executed"}
			closed := false
			err = runBrokerImageReloadUsing(b, r, brokerimage.StoreShutdown{Prepare: func(d controlbudget.Deadline) error {
				if err := r.PrepareNodeShutdown(n, d); err != nil {
					return err
				}
				if drift == "membership" {
					return n.AddNonvoter("joining", "joining:7400")
				}
				m := &b.cl.homeMaintenance
				m.mu.Lock()
				m.cutRecord.CutID, m.cutRecord.Phase, m.cutDeadline = "expired-during-prepare", brokermaint.MaintActive, 1
				m.mu.Unlock()
				return nil
			}, Close: func() error { closed = true; return n.Shutdown() }})
			if err == nil || closed || n.DB() != writer || b.nc.Load() != nc {
				t.Fatalf("final guard stranded original owner: err=%v closed=%v", err, closed)
			}
			if err := n.ApplyMetaSet("t:after-guard", "same-writer"); err != nil {
				t.Fatal(err)
			}
			if status := brokerImageSnapshot(b); status == nil || status.Phase != proto.BrokerImagePhaseControlReady {
				t.Fatalf("original admission did not resume: %+v", status)
			}
		})
	}
}
