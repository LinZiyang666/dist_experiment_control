//go:build linux

package drillobserve_test

import (
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferagent"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// Constructor observations belong to the successor even before the imported
// worker is returned to its host. No network attachment or byte work is needed
// to expose late binding: the real sleeping registry emits during Import.
// origin: S7 internal review INTEG-O F5
func TestTransferImportBindsGenerationBeforeConstructorObservation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferImportGeneration(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer import constructor generation", g)
	testharness.AssertNoFDLeak(t, "transfer import constructor generation", fd, 0)
}

func exerciseTransferImportGeneration(t *testing.T) {
	t.Helper()
	r, err := xferagent.New(xferagent.Host{SID: "session", NID: func() string { return "node" },
		Instance: func() string { return "instance" }, JetStream: func() jetstream.JetStream { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		t.Fatal(err)
	}
	cfg := r.WorkerConfig()
	cfg.Identity.AgentInstance = "instance"
	var observed []uint64
	cfg.Observe = func(x.Observation) { observed = append(observed, r.Bindings().OwnerGeneration) }
	state := x.WorkerState{Schema: x.StateSchema, Lineage: x.Lineage{InstanceID: "instance", BootID: boot, Generation: 4},
		Admission: x.AdmissionState{Closed: true, Generation: 4}}
	manager, salvage, err := r.ImportWorker(cfg, state, nil)
	if err != nil || manager == nil || len(salvage.Resources) != 0 {
		t.Fatalf("real sleeping import failed: manager=%v salvage=%v err=%v", manager != nil, len(salvage.Resources), err)
	}
	deadline, err := controlbudget.After(1e9)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Stop(deadline); err != nil {
		t.Fatal(err)
	}
	if len(observed) == 0 {
		t.Fatal("real import emitted no constructor observation")
	}
	for _, generation := range observed {
		if generation != 5 {
			t.Fatalf("constructor observation used generation %d; successor is 5", generation)
		}
	}
}
