//go:build linux

package agentimage

import (
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 internal review rv-image-3 / rv-authority-5
func TestFrozenBundleRequiresEveryRetainedReaderDomain(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseFrozenReaderDomains(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "retained reader domains", beforeG)
	testharness.AssertNoFDLeak(t, "retained reader domains", beforeFD, 0)
}

func exerciseFrozenReaderDomains(t *testing.T) {
	f := newBundleFixture(t, "final-reexport")
	client := liveImageFlow(t, execsession.SharedReplayBudget(f.rt))
	ss := ssproxy.New(testharness.SilentLog())
	if _, err := ss.Start(0, []ssproxy.Key{{KeyID: "reader-key", Secret: "reader-material"}}); err != nil {
		t.Fatal(err)
	}
	defer ss.Stop()
	cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Flow: client, SS: ss, Lineage: Lineage{Generation: 2}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(cut.Thaw)
	b := cut.Bundle()
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	full := CurrentCapabilities("reader-fixture")
	for _, domain := range []string{"flow-v1", "ss-v1"} {
		weak := Capabilities{ABI: 1}
		for _, d := range full.Domains {
			if d != domain {
				weak.Domains = append(weak.Domains, d)
			}
		}
		for _, readers := range [][]Capabilities{{weak, full}, {full, weak}, {Capabilities{}, full}, {full, Capabilities{}}} {
			if err := CheckBundleCapabilities(b, readers...); !errors.Is(err, ErrABIUnsupported) {
				t.Errorf("retained %s accepted unreadable tuple: %v", domain, err)
			}
			p, err := cut.PrepareChecked(func(final Bundle) error { return CheckBundleCapabilities(final, readers...) })
			if p != nil {
				_ = p.Close()
			}
			if !errors.Is(err, ErrABIUnsupported) {
				t.Errorf("final %s gate admitted unreadable tuple: %v", domain, err)
			}
		}
	}
	if err := CheckBundleCapabilities(b, full, full); err != nil {
		t.Fatal(err)
	}
	// Empty owners retain bare-exec compatibility, and every full tuple can prepare.
	if err := CheckBundleCapabilities(Bundle{}, Capabilities{}); err != nil {
		t.Fatal(err)
	}
	p, err := cut.PrepareChecked(func(final Bundle) error { return CheckBundleCapabilities(final, full, full) })
	if err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
}
