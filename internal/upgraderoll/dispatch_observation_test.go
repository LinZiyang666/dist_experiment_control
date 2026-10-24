package upgraderoll

import (
	"context"
	"fmt"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review cluster-roll F1
func TestParentWaitsForAnAcceptedChildWithoutRepeatingStart(t *testing.T) {
	m, p := newRollModel(t)
	m.startHook = func(upgradetx.Step) error {
		return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Accepted: true, Detail: "broker_activation_dispatched"}
	}
	if _, err := runModel(t, m, p); err != nil {
		t.Fatal("accepted child was not queried to completion:", err)
	}
	for _, n := range m.starts {
		if n != 1 {
			t.Fatalf("child dispatched %d times", n)
		}
	}
}

// origin: S8 internal review cluster-roll F2
func TestParentAcceptsEachOriginalGrantsOwnPublicationManifest(t *testing.T) {
	m, p := newRollModel(t)
	m.artifactHook = func(r *upgradeartifact.Receipt) { r.ManifestDigest = testSHA(800 + int(r.Grant.Handle.Generation)) }
	if _, err := Run(t.Context(), m, manifestPort{p}, m.plan); err != nil {
		t.Fatal("grant-bound publication was compared with a fleet manifest:", err)
	}
}

type manifestPort struct{ *rollPort }

func (p manifestPort) VerifyArtifact(_ context.Context, _ upgradetx.Plan, r upgradeartifact.Receipt) error {
	if r.ManifestDigest != testSHA(800+int(r.Grant.Handle.Generation)) {
		return fmt.Errorf("unbound manifest")
	}
	return nil
}
