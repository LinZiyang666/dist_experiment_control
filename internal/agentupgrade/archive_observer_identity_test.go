// Copy into internal/agentupgrade. Real store and engine; supplied death evidence
// names an independent observer. No child process or production state is used.
package agentupgrade

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradetx/contractstub"
)

type archiveObservation struct {
	contractstub.Runtime
	attempt upgradetx.Attempt
	dead    upgradetx.Owner
	last    upgradetx.DeathProof
}

func (r *archiveObservation) ObserveDeath(s upgradetx.Stamp) (upgradetx.DeathProof, error) {
	s.Producer = upgradetx.Owner{BootID: "observer-boot", PID: 222, StartTicks: 200, InstanceID: strings.Repeat("b", 32)}
	s.At = time.Now().UnixNano()
	s.Sequence = 1
	resources := append([]upgradetx.Resource{}, r.attempt.Scope.Expected...)
	for i := range resources {
		resources[i].State = "terminal"
		resources[i].Custody = "released"
	}
	data, _ := json.Marshal(r.attempt.Scope.Expected)
	sum := sha256.Sum256(data)
	c := upgradetx.Census{Stamp: s, Known: true, Complete: true, ExpectedDigest: hex.EncodeToString(sum[:]), Resources: resources}
	r.last = upgradetx.DeathProof{Stamp: s, Owner: r.dead, Reaped: true, NoOwnedFDs: true, Custody: c}
	return r.last, nil
}

// origin: S8 internal review rv-txn F12
func TestArchiveAcceptsIndependentDeathObserver(t *testing.T) {
	dir := t.TempDir()
	k := upgradetx.Key{DomainID: strings.Repeat("a", 32), Component: upgradetx.ComponentAgent, SID: "session", ConfiguredNID: "agent", ConfigPath: "/test/agent.yaml"}
	owner := upgradetx.Owner{BootID: "dead-boot", PID: 123, StartTicks: 100, InstanceID: strings.Repeat("a", 32)}
	domains := []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}
	scope := upgradetx.Scope{ID: strings.Repeat("b", 32), Participants: []upgradetx.Key{k}, Domains: domains}
	for _, d := range domains {
		scope.Expected = append(scope.Expected, upgradetx.Resource{Domain: d, ID: string(d), Origin: "original", Owner: owner, Generation: 1, State: "running", Custody: "source"})
	}
	store, err := upgradestore.Open(upgradestore.Options{Path: filepath.Join(dir, "state.json"), GuardPath: filepath.Join(dir, k.DomainID+".guard"), Key: k, Owner: owner, Transition: upgradetx.Transition})
	if err != nil {
		t.Fatal(err)
	}
	g, err := store.Prepare(k, upgradetx.PrepareRequest{ClientNonce: strings.Repeat("c", 32), ActorFP: "actor", SourceRelease: "A", TargetRelease: "B", SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), CapabilityDigest: strings.Repeat("c", 64), Mode: upgradetx.ModeHot, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.Start(g, upgradetx.Fence{Owner: owner, RunningSHA: g.SourceSHA})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &archiveObservation{attempt: record.Forward, dead: owner}
	endpoint := New(store, runtime, contractstub.Registry{})
	// ctx-root: hermetic transaction test entry point.
	_, engineErr := endpoint.Archive(context.Background(), record.Forward.Handle)
	if engineErr != nil {
		if _, err := store.Archive(record.Forward.Handle, runtime.last); err != nil {
			t.Fatalf("positive real-store death-proof premise failed: %v", err)
		}
		t.Fatalf("real store accepts independent death observer, but agent engine refuses: %v", engineErr)
	}
}
