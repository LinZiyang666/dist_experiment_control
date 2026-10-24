package xferserve

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

type originCensus struct {
	x.LedgerPort
	x.OwnerResolver
	value x.LedgerCensus
}

func (p originCensus) Scan(x.Deadline) (x.LedgerCensus, error)             { return p.value, nil }
func (p originCensus) Census(x.Origin, x.Deadline) (x.LedgerCensus, error) { return p.value, nil }

// A local ready census cannot prove absence on removed or unreachable origins.
// The production guard must refuse before creating any durable permit.
// origin: hot-update S7 plan §6.1 / §6.4 / T26 / T31
func TestTransferPermitRequiresCompleteOriginalOriginDiscovery(t *testing.T) {
	identity, _, err := LoadIdentity(t.TempDir(), "local")
	if err != nil {
		t.Fatal(err)
	}
	defer identity.Issuer.Wipe()
	root := t.TempDir()
	authority, err := xferauth.NewPermitAuthority(root, identity.Origin, identity.Issuer, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	local := originCensus{value: x.LedgerCensus{Origin: identity.Origin, Ready: true, Complete: true}}
	foreign := identity.Origin
	foreign.BrokerID, foreign.InstanceID = "foreign", "old-instance"
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer"}
	intent := x.Intent{Verb: "push", Tier: "a", Path: "/destination", Size: 0}
	base := Owners{Origin: identity.Origin, Ledger: func() x.LedgerPort { return local }}
	for _, reason := range []string{"missing discovery", "incomplete census", "prior root", "foreign protection"} {
		t.Run(reason, func(t *testing.T) {
			owners := base
			switch reason {
			case "missing discovery":
				owners.Remote = local
			case "incomplete census":
				owners.Ledger = func() x.LedgerPort { return originCensus{} }
			case "prior root":
				owners.PreviousRoots = []string{"unreconciled-root"}
			case "foreign protection":
				owners.Peers = func(x.Deadline) ([]x.Origin, error) { return []x.Origin{foreign}, nil }
				owners.Remote = originCensus{value: x.LedgerCensus{Origin: foreign, Ready: true, Complete: true,
					Entries: []x.Protection{{Identity: id}}}}
			}
			guard := GuardPermits{PermitAuthority: authority, Owners: owners}
			if _, err := guard.IssueFor(id, intent, assemblyDeadline(t)); err == nil || !strings.Contains(err.Error(), x.CodeOwnerRecovering) {
				t.Fatalf("issued through %s: %v", reason, err)
			}
			entries, err := os.ReadDir(root)
			if err != nil || len(entries) != 0 {
				t.Fatalf("refusal wrote a permit: %v %v", entries, err)
			}
		})
	}
	guard := GuardPermits{PermitAuthority: authority, Owners: base}
	if _, err := guard.IssueFor(id, intent, assemblyDeadline(t)); err != nil {
		t.Fatalf("complete single-origin census refused: %v", err)
	}
}

// The second restart must retain uncertainty from the first reboot too.
// origin: hot-update S7 plan §5.3 / §6.4 / T29
func TestTransferPreviousOriginRootsSurviveRepeatedRestarts(t *testing.T) {
	base := t.TempDir()
	id, _, err := LoadIdentity(base, "broker")
	if err != nil {
		t.Fatal(err)
	}
	id.Issuer.Wipe()
	old := filepath.Join(base, "older-instance")
	if err := os.Mkdir(old, 0700); err != nil {
		t.Fatal(err)
	}
	for restart := 0; restart < 3; restart++ {
		again, prior, err := LoadIdentity(base, "broker")
		if err != nil {
			t.Fatal(err)
		}
		again.Issuer.Wipe()
		if again.Origin != id.Origin || len(prior) != 1 || prior[0] != old {
			t.Fatalf("restart %d lost original roots: %+v %v", restart, again.Origin, prior)
		}
	}
}
