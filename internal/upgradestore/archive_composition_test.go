package upgradestore_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type composedDeathObserver struct {
	*composedRuntime
	defect string
}

func (r *composedDeathObserver) ObserveDeath(s upgradetx.Stamp) (upgradetx.DeathProof, error) {
	a := r.current()
	c := r.census(s, a.Scope)
	c.Stamp.Producer.PID++
	c.Stamp.Producer.InstanceID = txnHex("e", 32)
	for i := range c.Resources {
		c.Resources[i].State = "terminal"
		c.Resources[i].Custody = "released"
	}
	d := upgradetx.DeathProof{Owner: r.owner, Stamp: c.Stamp, Custody: c, Reaped: true, NoOwnedFDs: true}
	switch r.defect {
	case "pid_reuse":
		d.Owner.StartTicks++
	case "pid_only":
		d.Reaped = false
	case "retained_fd":
		d.NoOwnedFDs = false
	case "descendant":
		d.Custody.OldOwners = []upgradetx.Owner{r.owner}
	case "another_cut":
		d.Stamp.Attempt.CutID = txnHex("1", 32)
	case "dead_producer":
		d.Stamp.Producer = d.Owner
	case "stale":
		d.Stamp.At = time.Now().Add(-time.Hour).UnixNano()
	case "missing_resource":
		d.Custody.Resources = nil
	}
	d.Custody.Stamp = d.Stamp
	return d, nil
}

// origin: S8 internal review rv-txn F12
func TestBothEnginesArchiveOnlyIndependentCompleteDeathProof(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(role), func(t *testing.T) {
			r := newComposedRuntime(t, role)
			record, err := r.store.Start(r.grant, upgradetx.Fence{Owner: r.owner, RunningSHA: r.running})
			if err != nil {
				t.Fatal(err)
			}
			observer := &composedDeathObserver{composedRuntime: r}
			endpoint := agentupgrade.New(r.store, observer, r.registry)
			if role == upgradetx.ComponentBroker {
				endpoint = brokerupgrade.New(r.store, observer, r.registry)
			}
			// ctx-root: trusted local death observer boundary, no child process.
			ctx := context.Background()
			for _, defect := range []string{"pid_reuse", "pid_only", "retained_fd", "descendant", "another_cut", "dead_producer", "stale", "missing_resource"} {
				t.Run(defect, func(t *testing.T) {
					observer.defect = defect
					if _, err := endpoint.Archive(ctx, record.Forward.Handle); err == nil {
						t.Fatal("unproven death archived")
					}
					got, err := r.store.Load(r.options.Key, "")
					if err != nil || !reflect.DeepEqual(got.Historical, record) {
						t.Fatal("invalid death proof changed current")
					}
				})
			}
			observer.defect = ""
			v, err := endpoint.Archive(ctx, record.Forward.Handle)
			if err != nil || v.Historical.Forward.Phase != upgradetx.PhaseAbandoned {
				t.Fatalf("independent observer refused: %v", err)
			}
			r.reopen()
			got, err := r.store.Load(r.options.Key, record.Grant.Handle.TxID)
			if err != nil || !reflect.DeepEqual(got.Historical, v.Historical) {
				t.Fatalf("archive not durable: %v", err)
			}
		})
	}
}

// origin: S8 internal review rv-txn F07
func TestAgentIdentityComposesWithCutAndArtifact(t *testing.T) {
	r := newComposedRuntime(t, upgradetx.ComponentAgent)
	state := upgradecut.State{Attempt: r.grant.Handle, SourceSHA: r.grant.SourceSHA, TargetSHA: r.grant.TargetSHA, Owner: r.owner, LockFD: 1, Scope: r.scope, Authorities: []upgradecut.Authority{{SID: r.options.Key.SID, Incarnation: txnHex("f", 64)}}}
	encoded, err := upgradecut.Encode(state)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := upgradecut.Decode(encoded)
	if err != nil || !reflect.DeepEqual(decoded, state) {
		t.Fatalf("registered agent identity does not round trip: %v", err)
	}
	for _, edit := range []func(*upgradetx.Key){func(k *upgradetx.Key) { k.SID = "" }, func(k *upgradetx.Key) { k.Component = "unknown" }, func(k *upgradetx.Key) { k.ConfigPath = "relative" }} {
		key := r.options.Key
		edit(&key)
		if upgradetx.ValidateKey(key) == nil {
			t.Fatal("invalid role identity accepted")
		}
	}
}
