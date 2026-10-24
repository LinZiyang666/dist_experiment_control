package adminsock

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// origin: S7 internal review GATE-CONVERGE-layering F2
func TestAdminUpgradeKeepsReadAndControlOwners(t *testing.T) {
	for _, op := range []string{upgradewire.OpPrepare, upgradewire.OpStart, upgradewire.OpRollback} {
		t.Run(op, func(t *testing.T) {
			control, stopControl := context.WithCancel(t.Context())
			defer stopControl()
			read, stopReads := context.WithCancel(control)
			defer stopReads()
			s := &Server{controlCtx: control, readCtx: read}
			// The existing strict codec owns envelope validation. Encode below
			// selects a real valid fixture; this port only observes lifetime custody.
			request := validUpgradeLifetimeRequest(t, op)
			s.backend.UpgradeEndpoint = func(_ []byte, _ upgradewire.Access, deadline controlbudget.Deadline, stop <-chan struct{}) upgradewire.Response {
				if _, err := controlbudget.Remaining(deadline); err != nil {
					t.Fatal(err)
				}
				stopReads()
				select {
				case <-stop:
					if op != upgradewire.OpPrepare {
						t.Fatal("accepted cut lost control/reply custody to the read fence")
					}
				default:
					if op == upgradewire.OpPrepare {
						t.Fatal("read operation ignored the read fence")
					}
				}
				stopControl()
				select {
				case <-stop:
				default:
					t.Fatal("upgrade port ignored its terminal control owner")
				}
				return upgradewire.Response{Code: "observed"}
			}
			response := dispatchUpgrade(s, Request{Op: op, Upgrade: &request})
			if response.Upgrade == nil || response.Upgrade.Code != "observed" {
				t.Fatalf("upgrade did not reach its lifetime port: %+v", response)
			}
		})
	}
}

func validUpgradeLifetimeRequest(t *testing.T, op string) upgradewire.Request {
	t.Helper()
	// The wire package's production validation is retained, including operation
	// specific fields. Fixtures use the same declaration shapes as real requests.
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: "dddddddddddddddddddddddddddddddd", ConfigPath: "/fixture/broker.yaml"}
	grant := upgradetx.Grant{
		Handle:      upgradetx.Handle{Key: key, TxID: strings.Repeat("a", 32), CutID: strings.Repeat("b", 32), Generation: 2, Revision: 3},
		ClientNonce: "nonce", Issuer: upgradetx.Owner{BootID: "boot", PID: 12, StartTicks: 34, InstanceID: "instance"}, ActorFP: "actor",
		Mode: upgradetx.ModeHot, SourceRelease: "v1", TargetRelease: "v2", SourceSHA: strings.Repeat("1", 64), TargetSHA: strings.Repeat("2", 64),
		ScopeDigest: strings.Repeat("3", 64), CapabilityDigest: strings.Repeat("4", 64), ExpiresAt: time.Now().Add(time.Minute).UnixNano(),
	}
	scope := upgradetx.Scope{ID: "scope", Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainControl}}
	request := upgradewire.Request{Op: op, Key: key}
	switch op {
	case upgradewire.OpPrepare:
		request.Prepare = upgradetx.PrepareRequest{ClientNonce: grant.ClientNonce, ActorFP: grant.ActorFP, Mode: grant.Mode,
			SourceRelease: grant.SourceRelease, TargetRelease: grant.TargetRelease, SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA,
			CapabilityDigest: grant.CapabilityDigest, Scope: scope, ExpiresAt: grant.ExpiresAt}
	case upgradewire.OpStart:
		request.Grant = grant
		request.Artifact = upgradeartifact.Receipt{Grant: grant, CanonicalPath: "/fixture/tether", SourcePath: "/fixture/source", TargetPath: "/fixture/target",
			SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA, ManifestDigest: strings.Repeat("6", 64)}
	case upgradewire.OpRollback:
		request.Expected = upgradetx.Fence{Current: grant.Handle, Owner: grant.Issuer, RunningSHA: grant.TargetSHA, PreviousSHA: grant.SourceSHA}
		request.Reverse = upgradetx.ReverseRequest{ClientNonce: grant.ClientNonce, ActorFP: grant.ActorFP, HistoricalTxID: grant.Handle.TxID,
			TargetSHA: grant.SourceSHA, Scope: scope, ExpiresAt: grant.ExpiresAt}
	default:
		t.Fatalf("unhandled lifetime operation %s", op)
	}
	return request
}
