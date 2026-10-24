package infraattach

import (
	"github.com/LinZiyang666/tether/internal/session"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

func fixtureIdentitySupply(f *brokerAttachFixture) *IdentitySupply {
	auth := f.config(nil).Infra.Auth
	return &IdentitySupply{SID: attachSID, NID: attachNID, AccountPublicKey: auth.AccountPublicKey,
		Member: OrdinaryProbe{PublicKey: auth.Member.PublicKey, Sign: auth.Member.Sign, Subject: auth.Member.RPCSubject,
			Request: auth.Member.RPCRequest, Response: auth.Member.RPCResponse, Handle: auth.Business[0].Handle},
		Agent: OrdinaryProbe{PublicKey: auth.Agent.PublicKey, Sign: auth.Agent.Sign, Subject: auth.Agent.RPCSubject,
			Request: auth.Agent.RPCRequest, Response: auth.Agent.RPCResponse, Handle: auth.Business[1].Handle}}
}

// origin: hot-update S9 plan §3.3 / §4.4 / WIRE-2 R9
func TestBrokerIdentitySupplyRequiresExistingEnrollmentAndSigner(t *testing.T) {
	f := newBrokerAttachFixture(t, false)
	for _, scenario := range []string{"enrolled", "other-member", "other-agent", "other-session", "other-node", "wrong-signer", "missing-handler", "ambiguous-source"} {
		t.Run(scenario, func(t *testing.T) {
			supply := fixtureIdentitySupply(f)
			config := &BrokerInfra{Identities: supply}
			switch scenario {
			case "other-member":
				supply.Member.PublicKey = f.pubs["broker"]
				supply.Member.Sign = f.keys["broker"].Sign
			case "other-agent":
				supply.Agent.PublicKey = f.pubs["broker"]
				supply.Agent.Sign = f.keys["broker"].Sign
			case "other-session":
				supply.SID = "unprovisioned"
			case "other-node":
				supply.NID = "unprovisioned"
			case "wrong-signer":
				supply.Agent.Sign = f.keys["broker"].Sign
			case "missing-handler":
				supply.Member.Handle = nil
			case "ambiguous-source":
				config.Auth = f.config(nil).Infra.Auth
			}
			resolved, err := ResolveBrokerInfra(config, f.db)
			if scenario != "enrolled" {
				if err == nil || resolved != nil {
					t.Fatal("unproven production enrollment acquired an identity descriptor")
				}
				return
			}
			if err != nil || resolved.Auth == nil || resolved.Identities != nil || config.Auth != nil || config.Identities != supply {
				t.Fatalf("identity resolution mutated startup input or lost the descriptor: %v", err)
			}
			if resolved.Auth.Member.PublicKey != supply.Member.PublicKey || resolved.Auth.Agent.PublicKey != supply.Agent.PublicKey ||
				resolved.Auth.Member.Name != "tether-cli:"+attachSID || resolved.Auth.Agent.Name != "tether-agent:"+attachSID+":"+attachNID || len(resolved.Auth.Business) != 2 {
				t.Fatal("production descriptor did not bind both ordinary identities and RPCs")
			}
		})
	}
	t.Run("tombstoned-enrollment", func(t *testing.T) {
		if err := session.Tombstone(f.db, attachSID, time.Now()); err != nil {
			t.Fatal(err)
		}
		if resolved, err := ResolveBrokerInfra(&BrokerInfra{Identities: fixtureIdentitySupply(f)}, f.db); err == nil || resolved != nil {
			t.Fatal("tombstoned ordinary identities were revived by the supply")
		}
	})
	if got, err := ResolveBrokerInfra(nil, nil); got != nil || err != nil {
		t.Fatal("legacy startup changed")
	}
	if got, err := ResolveBrokerInfra(&BrokerInfra{Identities: fixtureIdentitySupply(f)}, nil); got != nil || err != c.ErrIncomplete {
		t.Fatal("missing current store accepted")
	}
}
