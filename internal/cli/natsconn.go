package cli

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

// ConnectNATSWithNkey opens a NATS connection associated with the given
// identity.
//
// **P3 transitional**: a default NATS server doesn't accept nkey-style
// CONNECT unless it has been configured with either an Nkeys user list
// (decentralized creds JWT) or auth_callout. P3 doesn't yet wire any of
// those, so this function intentionally does NOT pass `nats.Nkey(...)` —
// the connection is anonymous at the NATS layer and the identity's actor
// token (id.PublicKey) is only used to construct subjects.
//
// P3.5+ will add `nats.Nkey(id.PublicKey, signFromSeed(id.Seed))` here so
// the broker (via auth_callout) can authoritatively identify the
// connection. Until then, by.<actor> in subjects is a routing label, not
// proof of identity (architecture B.2 trust note).
func ConnectNATSWithNkey(url string, id *Identity, opts ...nats.Option) (*nats.Conn, error) {
	if id == nil {
		return nil, fmt.Errorf("cli: nil identity")
	}
	all := []nats.Option{
		nats.Name(fmt.Sprintf("tether-cli/%s", id.Fingerprint)),
		nats.MaxReconnects(-1),
	}
	all = append(all, opts...)
	return nats.Connect(url, all...)
}
