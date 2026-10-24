//go:build linux

// cut_private_owner_test.go — a sealed private FD exported by the tunnel leaf
// has exactly one closer across repeated final exports and current rollback.
package brokerimage

import (
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// ownedStateServer reproduces tunnel.serverFrozenImage's ownership contract
// verbatim: every Export seals a replacement and closes the previous snapshot
// itself (server_image.go Export: old := f.state; f.state = next; old.Close()),
// and Thaw/Discard close the last one (f.state.Close()).
type ownedStateServer struct {
	state  *sealedstate.Snapshot
	closes int
}

func (s *ownedStateServer) Export() (tunnel.ServerImage, []handoff.Resource, error) {
	next, err := sealedstate.New("tunnel.state", []byte{1})
	if err != nil {
		return tunnel.ServerImage{}, nil, err
	}
	old := s.state
	s.state = next
	if old != nil {
		s.closes++
		_ = old.Close()
	}
	return tunnel.ServerImage{Schema: TunnelStateRecord, SchemaVersion: 1, Generation: 1, StateResource: "tunnel.state"}, next.Resources, nil
}
func (s *ownedStateServer) Thaw(controlbudget.Deadline) error { s.closes++; return s.state.Close() }
func (s *ownedStateServer) Discard() error                    { s.closes++; return s.state.Close() }

func ownedStatePrepare(t *testing.T, cut Cut, bundle Bundle, resources []handoff.Resource) (*handoff.Prepared, error) {
	t.Helper()
	bundle.Generation = 2
	bundle.InstanceID, bundle.BootID = "instance", "boot"
	bundle.ExporterSHA256, bundle.CandidateSHA256 = strings.Repeat("a", 64), strings.Repeat("b", 64)
	bundle.PreviousSHA256, bundle.ConfigDigest = bundle.ExporterSHA256, strings.Repeat("c", 64)
	bundle.Args, bundle.StateResource = []string{"tether", "serve"}, "broker.state"
	snapshot, err := sealedstate.New(bundle.StateResource, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	prepared, err := cut.Prepare(bundle, append(resources, snapshot.Resources...))
	if prepared != nil {
		t.Cleanup(func() { _ = prepared.Close() })
	}
	return prepared, err
}

// ReloadCurrentStore always calls cut.Export(seal) and then cut.Prepare, and
// Prepare re-exports the server; an exec error then rolls back through
// Cut.Thaw. The cut registers the tunnel's sealed-state file as its own
// private owner (privateFiles) and closes it again in exportLocked and Thaw —
// after the tunnel leaf has already closed it. os.File returns ErrClosed for
// the second Close, which the cut reports as a failed private close: Prepare
// blocks a cut whose store is already closed, and Thaw fails after it has
// already restarted business.
// origin: hot-update S6 internal review rv-fd
func TestBrokerCutPrivateStateHasOneCloser(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCutPrivateOwner(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker leaf snapshot ownership", beforeG)
	testharness.AssertNoFDLeak(t, "broker leaf snapshot ownership", beforeFD, 0)
}

func exerciseCutPrivateOwner(t *testing.T) {
	t.Helper()
	t.Run("export-then-prepare", func(t *testing.T) {
		f := newCutFixture(t)
		server := &ownedStateServer{}
		t.Cleanup(func() { _ = server.state.Close() })
		f.cfg.FreezeServer = func(*tunnel.Server, controlbudget.Deadline) (tunnel.ServerFrozen, error) {
			return server, nil
		}
		cut := freezeFixture(t, f)
		bundle, resources := exportFixture(t, f, cut)
		if _, err := ownedStatePrepare(t, cut, bundle, resources); err != nil {
			t.Fatalf("final Prepare after Export refused (tunnel leaf closed its replaced state %d time(s)): %v", server.closes, err)
		}
		if cut.(*frozenCut).blocked {
			t.Fatal("cut left blocked after a successful final Prepare")
		}
	})
	t.Run("exec-error-thaw", func(t *testing.T) {
		f := newCutFixture(t)
		server := &ownedStateServer{}
		t.Cleanup(func() { _ = server.state.Close() })
		f.cfg.FreezeServer = func(*tunnel.Server, controlbudget.Deadline) (tunnel.ServerFrozen, error) {
			return server, nil
		}
		slot, _ := runtimeFixture(t, f)
		current := &cutStore{events: f.events, stamp: cloneStamp(f.store.stamp), lock: f.store.lock}
		f.store.reopen = current
		f.cfg.NewControl = func(StoreOwner, controlbudget.Deadline) (ControlOwner, error) {
			return &cutControl{events: f.events, name: "current"}, nil
		}
		cut := freezeFixture(t, f)
		if err := BindRuntime(cut, slot); err != nil {
			t.Fatal(err)
		}
		bundle, resources := exportFixture(t, f, cut)
		if _, err := ownedStatePrepare(t, cut, bundle, resources); err != nil {
			t.Fatal(err)
		}
		finalState := server.state.Owned[0]
		var brokerState []*os.File
		for file := range cut.(*frozenCut).graph.private {
			brokerState = append(brokerState, file)
		}
		if len(brokerState) != 1 {
			t.Fatalf("cut must own only one broker private state file: %d", len(brokerState))
		}
		// Rollback of a closed-store cut (Prepare failure or Exec error).
		if err := cut.Thaw(f.cfg.Deadline); err != nil {
			t.Fatalf("current rollback reported failure after resuming business (tunnel closes=%d): %v; events=%v",
				server.closes, err, f.events.snapshot())
		}
		for _, file := range append(brokerState, finalState) {
			if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("rollback did not release its private state: %v", err)
			}
		}
	})
}
