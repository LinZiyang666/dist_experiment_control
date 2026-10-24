package brokerupgrade

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review cluster-roll F3
func TestBrokerQueriesPreparedGrantsWithoutAcceptingThem(t *testing.T) {
	f := newEngineFixture()
	key := f.artifact.Grant.Handle.Key
	dir := t.TempDir()
	store, err := upgradestore.Open(upgradestore.Options{Path: filepath.Join(dir, "upgrade-state.json"), GuardPath: filepath.Join(dir, key.DomainID+".guard"), Key: key, Owner: f.artifact.Grant.Issuer, Transition: upgradetx.Transition})
	if err != nil {
		t.Fatal(err)
	}
	req := upgradetx.PrepareRequest{ClientNonce: strings.Repeat("a", 32), ActorFP: "actor", Mode: upgradetx.ModeHot, SourceRelease: "v1", TargetRelease: "v2", SourceSHA: f.artifact.Grant.SourceSHA, TargetSHA: f.artifact.Grant.TargetSHA, CapabilityDigest: strings.Repeat("c", 64), Scope: f.store.scope, ExpiresAt: time.Now().Add(time.Minute).UnixNano()}
	endpoint := New(store, f.runtime, f.registry)
	grant, err := endpoint.Prepare(t.Context(), key, req)
	if err != nil {
		t.Fatal(err)
	}
	nonce := strings.Repeat("e", 32)
	view, err := endpoint.Query(t.Context(), key, grant.Handle.TxID, nonce)
	if err != nil || view.Historical.Grant != grant || view.Historical.Forward.Phase != upgradetx.PhasePrepared || view.Observation.Nonce != nonce || !view.Observation.Census.Known {
		t.Fatalf("prepared source could not be queried: %v %+v", err, view)
	}
	if _, err = store.Load(key, grant.Handle.TxID); err == nil {
		t.Fatal("prepared Query accepted an execution")
	}
	for _, defect := range []string{"running", "owner", "incomplete"} {
		f.runtime.running = grant.SourceSHA
		f.runtime.observe = func(o *upgradetx.Observation) {
			if defect == "owner" {
				o.OwnerKnown = false
			}
			if defect == "incomplete" {
				o.Census.Complete = false
			}
		}
		if defect == "running" {
			f.runtime.running = grant.TargetSHA
		}
		if _, err = endpoint.Query(t.Context(), key, grant.Handle.TxID, nonce); err == nil {
			t.Fatalf("%s source was certified prepared", defect)
		}
	}
}
