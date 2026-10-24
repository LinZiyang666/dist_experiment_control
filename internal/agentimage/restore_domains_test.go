//go:build linux

package agentimage

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 internal review rv-image-1
func TestFrozenDomainsImportAndDetachCompleteDescriptors(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exercisePristineDomainImport(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "pristine domain import", beforeG)
	testharness.AssertNoFDLeak(t, "pristine domain import", beforeFD, 0)
}

func exercisePristineDomainImport(t *testing.T) {
	f := newBundleFixture(t, "final-reexport")
	client := liveImageFlow(t, execsession.SharedReplayBudget(f.rt))
	ss := ssproxy.New(testharness.SilentLog())
	if _, err := ss.Start(0, []ssproxy.Key{{KeyID: "detach-key", Secret: "detach-material"}}); err != nil {
		t.Fatal(err)
	}
	defer ss.Stop()
	cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Flow: client, SS: ss, Lineage: Lineage{Generation: 2}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(cut.Thaw)
	b := cut.Bundle()
	budget := &workload.ReplayBudget{}
	cfg := f.cfg
	cfg.Manager = &workload.Manager{}
	cfg.SharedReplayBudget = budget
	resources := cut.resourcesForTest()
	src := newBundleDupSource(t, resources)
	expected := map[string]*os.File{}
	for id, files := range src.files {
		for name, file := range files {
			expected[id+"/"+name] = file
		}
	}
	imp, salvage, err := Import(b, b.Processes.Records, src, Target{Runtime: cfg})
	if salvage != nil {
		defer func(closeFunc func() error) { _ = closeFunc() }(salvage.Close)
	}
	if err != nil {
		t.Fatalf("composed Flow/SS import refused: %v", err)
	}
	detached := false
	defer func() {
		if !detached {
			if s, _ := imp.Detach(); s != nil {
				_ = s.Close()
			}
		}
	}()
	if imp.Flow == nil || imp.SS == nil || budget.Used() != b.Flow.Flows.ReservedBytes {
		t.Fatal("domains or replay reservation missing")
	}
	salvage, err = imp.Detach()
	if salvage != nil {
		defer func(closeFunc func() error) { _ = closeFunc() }(salvage.Close)
	}
	if err != nil {
		t.Fatal(err)
	}
	detached = true
	if budget.Used() != 0 {
		t.Fatal("detach retained replay reservation")
	}
	// Domain Detach returns its taken descriptors; calibration stays with
	// the handoff owner. The redispatch closure must contain both, exactly once.
	// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility.
	var remainder []handoff.Resource
	for _, resource := range resources {
		if files := src.files[resource.ID]; len(files) != 0 {
			if resource.ID != calibrationResourceID || len(files) != 1 || files["state"] == nil {
				t.Fatal("domain detach left business custody behind")
			}
			resource.Files = files
			remainder = append(remainder, resource)
		}
	}
	if len(remainder) != 1 {
		t.Fatal("handoff lost calibration custody")
	}
	complete, err := handoff.MergeResources(salvage.Resources, remainder)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]*os.File{}
	for _, res := range complete {
		for name, file := range res.Files {
			key := res.ID + "/" + name
			if got[key] != nil || expected[key] != file {
				t.Fatal("detach changed original descriptor closure")
			}
			if _, err := file.Stat(); err != nil {
				t.Fatalf("detach closed retained descriptor: %v", err)
			}
			got[key] = file
		}
	}
	if len(got) != len(expected) {
		t.Fatal("detach omitted sealed or business descriptors")
	}
	p, err := salvage.Prepare(remainder...)
	if err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
	// Re-import the exact original cut to prove its descriptor references remain usable.
	again := newBundleDupSource(t, complete)
	cfg.Manager = &workload.Manager{}
	restored, refused, err := Import(b, b.Processes.Records, again, Target{Runtime: cfg})
	if refused != nil {
		defer func(closeFunc func() error) { _ = closeFunc() }(refused.Close)
	}
	if err != nil {
		t.Fatal(err)
	}
	s, err := restored.Detach()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(s.Close)
}
