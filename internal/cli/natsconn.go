package cli

import (
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// DevNoAuthEnv switches the CLI off nkey CONNECT.
//
// Set TETHER_DEV_NO_AUTH=1 to make `tether session/login/exec/ps`
// connect anonymously to a vanilla `nats-server` (no auth_callout, no
// `Nkeys` config). This exists for local laptop demos / debugging only.
//
// Security: with this set, NATS does not pin the JWT `by.<actor>`
// segment, so the broker treats the actor in subjects as a routing
// label, not a proven identity (P3 trust degrades to P2-era).
// Application-layer membership/owner checks still run, but anyone with
// network access to the broker can spoof another actor's fingerprint.
// Never enable in production.
const DevNoAuthEnv = "TETHER_DEV_NO_AUTH"

// ConnectNATSWithNkey opens a NATS connection using the identity's user
// nkey for the CONNECT challenge. Callers add `nats.Name(...)` (carrying
// role + session — see internal/authcallout.parseRole) and optionally
// `nats.Token(pin)` for first-time PIN join via `opts`.
//
// With auth_callout enabled on the broker side (architecture B.2 / E.2),
// the CONNECT triggers a server→broker auth callout. NATS pins the
// user's nkey into the issued JWT permissions, so subject `by.<actor>`
// segments published from this conn are unforgeable.
//
// When DevNoAuthEnv is non-empty, this function omits nats.Nkey() and
// falls back to anonymous CONNECT — see DevNoAuthEnv doc.
func ConnectNATSWithNkey(url string, id *Identity, opts ...nats.Option) (*nats.Conn, error) {
	if id == nil {
		return nil, fmt.Errorf("cli: nil identity")
	}
	all := []nats.Option{nats.MaxReconnects(-1)}
	if os.Getenv(DevNoAuthEnv) == "" {
		seed := append([]byte(nil), id.Seed...) // captured by sigCB closure
		sigCB := func(nonce []byte) ([]byte, error) {
			kp, err := nkeys.FromSeed(seed)
			if err != nil {
				return nil, fmt.Errorf("cli: nkey from seed: %w", err)
			}
			defer kp.Wipe()
			return kp.Sign(nonce)
		}
		all = append(all, nats.Nkey(id.PublicKey, sigCB))
	}
	all = append(all, opts...)
	return nats.Connect(url, all...)
}

// ResolveNATSURLFromHome implements the broker-URL precedence chain
// the ctl uses on every command. flagVal is whatever cobra parsed
// (cobra default unless the user passed --nats-url); flagChanged is
// Cmd.Flags().Changed("nats-url"). Returns the URL the caller should
// hand to ConnectNATSWithNkey.
//
// Precedence (high → low):
//  1. explicit --nats-url flag (operator override always wins)
//  2. $TETHER_NATS_URL env var (per-shell override)
//  3. ~/.tether/broker_url file (persistent default written by `tether login --broker`)
//  4. flagVal (cobra default — typically nats://127.0.0.1:4222)
//
// Call this in every ctl subcommand's RunE before connecting.
func ResolveNATSURLFromHome(flagVal string, flagChanged bool, home string) string {
	if flagChanged {
		return flagVal
	}
	if u := ReadDefaultBrokerURL(home); u != "" {
		return u
	}
	return flagVal
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

// AgentName is the connection-Name an agent uses on CONNECT. The
// auth_callout handler parses this format ("tether-agent:<sid>:<nid>")
// to bind the connecting nkey to the (sid, nid) registered in
// agent_provisioning (architecture E.2 / K.1). Real agents always
// connect with this Name + their nkey; the broker's auth_callout
// branch verifies the binding before issuing a JWT.
func AgentName(sid, nid string) string { return "tether-agent:" + sid + ":" + nid }
