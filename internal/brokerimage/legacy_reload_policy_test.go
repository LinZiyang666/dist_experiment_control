//go:build linux

package brokerimage

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// origin: S8 internal review rv3-leaves F2
func TestLegacyReloadBackendsCannotArmWithoutGrant(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseLegacyReloadBackends(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "legacy reload backend x5", g)
	testharness.AssertNoFDLeak(t, "legacy reload backend x5", fd, 0)
}

func exerciseLegacyReloadBackends(t *testing.T) {
	t.Helper()
	sha, err := StagedArtifactSHA256()
	if err != nil {
		t.Fatal(err)
	}
	req := adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v-new-999", ExpectSHA256: sha}
	armed := 0
	direct := HandleStoreReloadRequest(req, nil, func() { armed++ })
	if direct.OK || direct.Reloading || direct.Code != "try_again" || armed != 0 {
		t.Fatalf("old cluster backend bypassed policy: %+v armed=%d", direct, armed)
	}
	f := newCutFixture(t)
	slot, err := NewRuntimeSlot(func(StoreOwner, controlbudget.Deadline) error { return nil }, func(tunnel.ServerImported, controlbudget.Deadline) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	run := &StoreRun[int]{Slot: slot, Capture: func(int, controlbudget.Deadline) (Config, error) { return f.cfg, nil }, Freeze: Freeze,
		Preflight: func(int, controlbudget.Deadline) error { return nil }, PrepareNodeShutdown: func(*cluster.Node, controlbudget.Deadline) error { return nil },
		InstallCurrent: func(int) error { return nil }, Recover: func(int, Cut, storage.StoreSeal, controlbudget.Deadline) error { return nil },
		DrainArmed: func(controlbudget.Deadline) error { return nil }, WithExecLatch: func(fn func() error) error { return fn() }, CandidatePath: "/unused"}
	var gate sync.Mutex
	var signal StoreReloadSignal
	admissions, resumed := 0, 0
	local := &LocalStoreReload[int]{Image: run, Gate: &gate, Signal: &signal, Status: func(int) *proto.BrokerImageStatus { return nil },
		Fence: func(int, controlbudget.Deadline) (uint32, error) { return 0, nil }, Resume: func(int) { resumed++ },
		Admission: func(int, adminsock.Request, controlbudget.Deadline) error { admissions++; return nil }}
	response := local.HandleCluster(req)
	if response.OK || response.Reloading || response.Code != "try_again" || signal.Requested() || admissions != 0 || resumed != 1 {
		t.Fatalf("local backend reached cut admission before policy: %+v admission=%d resumed=%d", response, admissions, resumed)
	}
}
