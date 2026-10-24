package agentupgradeport

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review FREEZE-WINDOW R6
// Reader declarations depend on verified bytes. Re-executing the same probe
// in the final preflight spends the freeze budget without adding evidence;
// retained-domain policy and the current census must still be checked afresh.
func TestAgentPreflightProbesUnchangedBytesBeforeFreeze(t *testing.T) {
	dir := t.TempDir()
	source, target := hexOf([]byte("source")), hexOf([]byte("target"))
	pin := filepath.Join(dir, target)
	if err := os.WriteFile(pin, []byte("target"), 0555); err != nil {
		t.Fatal(err)
	}
	owner := upgradetx.Owner{BootID: "boot", PID: os.Getpid(), StartTicks: 7, InstanceID: strings.Repeat("9", 32)}
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node"}
	grant := upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 1}, Issuer: owner, Mode: upgradetx.ModeHot, SourceRelease: "source", TargetRelease: "target", SourceSHA: source, TargetSHA: target, ScopeDigest: strings.Repeat("5", 64)}
	reader := func(sha string) agentimage.Capabilities { return agentimage.CurrentCapabilities(sha) }
	probes, policies, snapshots := 0, 0, 0
	port := NewRuntime(Host{Key: key, Journal: pinJournal{tx: grant.Handle.TxID, pins: []string{pin}},
		Owner:      func() (upgradetx.Owner, error) { return owner, nil },
		RunningSHA: func() (string, error) { return source, nil },
		Source:     func(sha string) (string, agentimage.Capabilities, error) { return "source", reader(sha), nil },
		Probe:      func(string) (string, agentimage.Capabilities, error) { probes++; return "target", reader(target), nil },
		Candidate:  func(agentimage.Capabilities) error { policies++; return nil },
		Deferrals:  func() []agentimage.DeferredError { return nil }, Required: func() []string { return []string{"exec-v1"} },
		Snapshot: func() (execsession.Image, agentimage.CourierState, error) {
			snapshots++
			return execsession.Image{}, agentimage.CourierState{}, nil
		},
		Transfers: noTransfers, Incarnation: func() string { return strings.Repeat("7", 64) },
	}).(*runtimePort)
	for gate := 0; gate < 3; gate++ {
		if _, err := port.Preflight(t.Context(), grant, upgradeartifact.Receipt{}); err != nil {
			t.Fatal(err)
		}
	}
	if probes != 1 || policies != 3 || snapshots != 3 {
		t.Fatalf("unchanged reader evidence: probes=%d policies=%d snapshots=%d, want 1/3/3", probes, policies, snapshots)
	}
}

// origin: S8 internal review FREEZE-WINDOW R6
// A digest lie and a failed probe must be retried. Neither may certify the
// expected bytes; a different digest must get its own isolated probe.
func TestAgentReaderEvidenceNeverCachesAProbeForOtherBytes(t *testing.T) {
	sha, other := hexOf([]byte("candidate")), hexOf([]byte("another candidate"))
	calls := 0
	port := NewRuntime(Host{Probe: func(string) (string, agentimage.Capabilities, error) {
		calls++
		switch calls {
		case 1:
			return "other", agentimage.CurrentCapabilities(other), nil
		case 2:
			return "candidate", agentimage.CurrentCapabilities(sha), os.ErrPermission
		}
		return "candidate", agentimage.CurrentCapabilities(sha), nil
	}}).(*runtimePort)
	for i := 0; i < 4; i++ {
		_, _, _ = port.candidateReader("candidate", sha)
		if i < 2 && port.probed.capabilities.SHA256 != "" {
			t.Fatal("an invalid probe populated reader evidence")
		}
	}
	if calls != 3 {
		t.Fatalf("failed or digest-mismatched probe was cached: probes=%d, want 3", calls)
	}
	_, caps, err := port.candidateReader("another path with verified equal bytes", sha)
	if err != nil {
		t.Fatal(err)
	}
	caps.Domains[0] = "poisoned"
	caps.ReadEpochs[0] = 0
	caps.Features[0] = "poisoned"
	_, again, err := port.candidateReader("candidate", sha)
	if err != nil || again.Domains[0] == "poisoned" || again.ReadEpochs[0] == 0 || again.Features[0] == "poisoned" {
		t.Fatal("a policy callback mutated cached declarations")
	}
	_, _, _ = port.candidateReader("other", other)
	if calls != 4 {
		t.Fatal("another digest borrowed the cached probe")
	}
}
