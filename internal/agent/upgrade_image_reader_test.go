//go:build linux

package agent

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 internal review rv-image-3 / rv-authority-5
func TestFinalPrepareRejectsFlowAdmittedAfterReaderPreflight(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseLateFlowReaderGate(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "late flow reader gate", beforeG)
	testharness.AssertNoFDLeak(t, "late flow reader gate", beforeFD, 0)
}

func exerciseLateFlowReaderGate(t *testing.T) {
	a := cutTestAgent(t)
	full := agentimage.CurrentCapabilities("fixture")
	if err := checkCandidateABI(a, agentimage.Capabilities{}); err != nil {
		t.Fatal(err)
	}
	adapter, port, target := flowDomainAdapter(t, a)
	public := flowFailureDial(t, port)
	peer, err := target.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(peer.Close)
	flowFailureExchange(t, public, peer, []byte("admitted-after-preflight"))
	if adapter.client.FlowCensus().Owned == 0 {
		t.Fatal("late flow was not admitted")
	}
	a.gate.close()
	cut, err := freezeForUpgrade(a, time.Now().Add(cutTestWait))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cut.abandon(a) }()
	dst := a.cfg.UpgradeExecutablePath
	if err := os.WriteFile(upgradePrevPath(dst), []byte("reader-prev"), 0755); err != nil {
		t.Fatal(err)
	}
	full.SHA256 = mustSHA(t, dst)
	prev := full
	prev.SHA256 = mustSHA(t, upgradePrevPath(dst))
	for _, abi := range []int{0, 1} {
		weak := agentimage.Capabilities{ABI: abi, Domains: []string{"exec-v1", "pty-v1", "ss-v1"}, SHA256: full.SHA256}
		for _, previousWeak := range []bool{false, true} {
			readers := []agentimage.Capabilities{weak, prev}
			if previousWeak {
				readers = []agentimage.Capabilities{full, weak}
				readers[1].SHA256 = prev.SHA256
			}
			cut.readers = readers
			if err := agentimage.CheckBundleCapabilities(cut.frozen.Bundle(), readers...); !errors.Is(err, errImageABIUnsupported) {
				t.Errorf("frozen gate admitted late flow: %v", err)
			}
			p, err := prepareImageCut(a, cut, func(b agentimage.Bundle) error { return checkUpgradeCutReaders(dst, b, readers) })
			if p != nil {
				_ = p.Close()
			}
			if !errors.Is(err, errImageABIUnsupported) {
				t.Errorf("Prepare admitted late flow: %v", err)
			}
			p, err = prepareImageCut(a, cut, nil)
			if p != nil {
				_ = p.Close()
			}
			if !errors.Is(err, errImageABIUnsupported) {
				t.Errorf("final gate without optional callback admitted late flow: %v", err)
			}
			if err := checkUpgradeCutReaders(dst, cut.frozen.Bundle(), readers); !errors.Is(err, errImageABIUnsupported) {
				t.Errorf("pre-exec gate admitted late flow: %v", err)
			}
		}
	}
	cut.readers = []agentimage.Capabilities{full, prev}
	p, err := prepareImageCut(a, cut, func(b agentimage.Bundle) error { return checkUpgradeCutReaders(dst, b, cut.readers) })
	if err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
}
