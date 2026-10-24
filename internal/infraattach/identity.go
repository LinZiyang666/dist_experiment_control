package infraattach

import (
	"crypto/rand"
	"database/sql"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/infraauth"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// OrdinaryProbe borrows the deployment's existing signer and an ordinary RPC
// binding. It contains no seed, PIN, registration mutation or bypass principal.
// The RPC handler is installed on the auth owner's business NC, not the NC
// whose admission is still being proven. Its response is a public exact probe.
type OrdinaryProbe struct {
	PublicKey string
	Sign      nats.SignatureHandler
	Subject   string
	Request   []byte
	Response  []byte
	Handle    nats.MsgHandler
}

// IdentitySupply is a memory-only startup input. Startup loads the already
// activated member and provisioned agent through its existing credential owner;
// this adapter never creates sessions, writes provisioning or enrolls with a PIN.
// origin: hot-update S9 plan §3.3 / §4.4 / WIRE-2 R9
type IdentitySupply struct {
	AccountPublicKey string
	SID              string
	NID              string
	Member           OrdinaryProbe
	Agent            OrdinaryProbe
	Authority        []infraauth.Binding
}

func suppliedIdentity(probe OrdinaryProbe, name string) (infraauth.Identity, infraauth.Binding, error) {
	if !nkeys.IsValidPublicUserKey(probe.PublicKey) || probe.Sign == nil || probe.Handle == nil ||
		probe.Subject == "" || len(probe.Response) == 0 {
		return infraauth.Identity{}, infraauth.Binding{}, c.ErrIncomplete
	}
	// Verify possession now; a descriptor cannot silently pair a valid public
	// enrollment with another principal's signer and advertise a working root.
	var challenge [32]byte
	if _, err := rand.Read(challenge[:]); err != nil {
		return infraauth.Identity{}, infraauth.Binding{}, c.ErrIncomplete
	}
	public, err := nkeys.FromPublicKey(probe.PublicKey)
	if err != nil {
		return infraauth.Identity{}, infraauth.Binding{}, c.ErrIncomplete
	}
	defer public.Wipe()
	sig, err := probe.Sign(challenge[:])
	if err != nil || public.Verify(challenge[:], sig) != nil {
		return infraauth.Identity{}, infraauth.Binding{}, c.ErrIncomplete
	}
	return infraauth.Identity{Name: name, PublicKey: probe.PublicKey, Sign: probe.Sign,
			RPCSubject: probe.Subject, RPCRequest: append([]byte(nil), probe.Request...), RPCResponse: append([]byte(nil), probe.Response...)},
		infraauth.Binding{Subject: probe.Subject, Handle: probe.Handle}, nil
}

// ResolveBrokerInfra binds production identities against the CURRENT store
// before any NC is opened, on cold and hot entry. Subsequent reconnects still
// authenticate through the ordinary callout, so revocation is never cached as
// admission. A partial descriptor is refused rather than silently going legacy.
// origin: hot-update S9 plan §3.3 / §4.4 / WIRE-2 R9
func ResolveBrokerInfra(config *BrokerInfra, db *sql.DB) (*BrokerInfra, error) {
	if config == nil || config.Identities == nil {
		return config, nil
	}
	s := config.Identities
	if db == nil || config.Auth != nil || !nkeys.IsValidPublicAccountKey(s.AccountPublicKey) ||
		proto.ValidateSID(s.SID) != nil || proto.ValidateNID(s.NID) != nil || s.Member.PublicKey == s.Agent.PublicKey ||
		s.Member.Subject == s.Agent.Subject {
		return nil, c.ErrIncomplete
	}
	memberFP, err := auth.FingerprintFromActor(s.Member.PublicKey)
	if err != nil {
		return nil, c.ErrIncomplete
	}
	agentFP, err := auth.FingerprintFromActor(s.Agent.PublicKey)
	if err != nil {
		return nil, c.ErrIncomplete
	}
	active, err := session.IsActive(db, s.SID)
	if err != nil || !active {
		return nil, c.ErrIncomplete
	}
	member, err := session.IsMember(db, s.SID, memberFP)
	if err != nil || !member {
		return nil, c.ErrIncomplete
	}
	provisioned, err := agentprov.Lookup(db, s.SID, s.NID)
	if err != nil || provisioned != agentFP {
		return nil, c.ErrIncomplete
	}
	m, mb, err := suppliedIdentity(s.Member, "tether-cli:"+s.SID)
	if err != nil {
		return nil, err
	}
	a, ab, err := suppliedIdentity(s.Agent, "tether-agent:"+s.SID+":"+s.NID)
	if err != nil {
		return nil, err
	}
	resolved := *config
	resolved.Identities = nil
	resolved.Auth = &BrokerAuth{AccountPublicKey: s.AccountPublicKey, Member: m, Agent: a,
		Authority: append([]infraauth.Binding(nil), s.Authority...), Business: []infraauth.Binding{mb, ab}}
	return &resolved, nil
}

// NewBrokerFromStore is the production cold/hot constructor: resolve ordinary
// enrollment before opening any transport, then freeze the owned descriptor.
func NewBrokerFromStore(db *sql.DB, config BrokerConfig) (*Broker, error) {
	resolved, err := ResolveBrokerInfra(config.Infra, db)
	if err != nil {
		return nil, err
	}
	config.Infra = resolved
	return NewBroker(config)
}
