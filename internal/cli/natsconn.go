package cli

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// ConnectNATSWithNkey opens a NATS connection using the identity's user
// nkey for the CONNECT challenge.
//
// Callers add `nats.Name(...)` (carrying role + session — see
// internal/authcallout.parseRole) and optionally `nats.Token(pin)` for
// first-time PIN join via `opts`.
//
// With auth_callout enabled on the broker side (architecture B.2 / E.2),
// the CONNECT triggers a server→broker auth callout. NATS pins the
// user's nkey into the issued JWT permissions, so subject `by.<actor>`
// segments published from this conn are unforgeable.
func ConnectNATSWithNkey(url string, id *Identity, opts ...nats.Option) (*nats.Conn, error) {
	if id == nil {
		return nil, fmt.Errorf("cli: nil identity")
	}
	seed := append([]byte(nil), id.Seed...) // captured by sigCB closure
	sigCB := func(nonce []byte) ([]byte, error) {
		kp, err := nkeys.FromSeed(seed)
		if err != nil {
			return nil, fmt.Errorf("cli: nkey from seed: %w", err)
		}
		defer kp.Wipe()
		return kp.Sign(nonce)
	}
	all := []nats.Option{
		nats.Nkey(id.PublicKey, sigCB),
		nats.MaxReconnects(-1),
	}
	all = append(all, opts...)
	return nats.Connect(url, all...)
}

// CtlNameUnactivated is the connection-Name a CLI uses before activating
// a session. The broker auth_callout decodes this and grants the
// "unactivated" permission template (architecture B.2).
const CtlNameUnactivated = "tether-cli"

// CtlNameForSession is the connection-Name a CLI uses to activate a
// session. The broker auth_callout extracts <sid> and grants the
// "activated member" permission template after a membership check (or
// PIN verify if a Token is also presented).
func CtlNameForSession(sid string) string { return "tether-cli:" + sid }

// AgentName is the connection-Name an agent uses. P3 transitional —
// auth_callout grants agent permissions without a membership check.
// P4+ tightens this when full agent identity lands.
func AgentName(sid, nid string) string { return "tether-agent:" + sid + ":" + nid }
