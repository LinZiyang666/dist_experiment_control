package cli

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
)

// Settlement connection state is private to the original receiver. A capsule
// does not grant byte IO and can never replace the data attachment's role.
// origin: hot-update S7 plan §6.4 / INTEG-D
type transferSettlement struct {
	mu      sync.Mutex
	capsule *x.SettlementCapsule
	nc      *nats.Conn
	closed  bool
}

func (t *TransferTransport) BindSettlement(c x.SettlementCapsule) error {
	t.settlement.mu.Lock()
	defer t.settlement.mu.Unlock()
	if c.ReceiverNkey != t.id.PublicKey || c.Identity.ActorFP != t.id.Fingerprint || c.Signature == "" || c.Nonce == "" || c.Issuer == "" ||
		(c.Identity.SID == "" || c.Identity.NID == "" || c.Identity.TransferID == "" || c.Identity.AgentInstance == "" || c.Identity.EntryNonce == "" || c.Identity.IntentDigest == "") || c.Origin.BrokerID == "" || c.Origin.InstanceID == "" || c.Origin.BootID == "" {
		return errors.New(x.CodeIntentConflict)
	}
	if t.settlement.closed || t.settlement.capsule != nil && *t.settlement.capsule != c {
		return errors.New(x.CodeIntentConflict)
	}
	copy := c
	t.settlement.capsule = &copy
	return nil
}

func (t *TransferTransport) requestConnection(r x.Request, d controlbudget.Deadline) (*nats.Conn, error) {
	if r.Op != x.OpTerminal && r.Op != x.OpFinalize && r.Op != x.OpStatus && r.Op != x.OpCancel {
		return t.control, nil
	}
	t.settlement.mu.Lock()
	defer t.settlement.mu.Unlock()
	s := &t.settlement
	if s.capsule == nil {
		return t.control, nil
	}
	if s.closed || r.Identity != s.capsule.Identity || r.Origin != s.capsule.Origin {
		return nil, errors.New(x.CodeIntentConflict)
	}
	// A settled row retires the capsule's CONNECT authority. The member's
	// read-only status route still returns its retained canonical terminal;
	// using a reconnecting settlement attachment here traps a lost terminal
	// ACK forever. A revoked member retains the restricted settlement route.
	// origin: S7 internal review FIX-TIERA2 F8 / hot-update S7 plan §6.1 / §6.4 / §7 / T46
	if r.Op == x.OpStatus && t.control.IsConnected() {
		return t.control, nil
	}
	if s.nc != nil {
		return s.nc, nil
	}
	payload, err := json.Marshal(s.capsule)
	if err != nil {
		return nil, errors.New("invalid settlement capsule")
	}
	// CONNECT still signs the server's nonce using the original NKey. The capsule
	// is a private token, never a command argument, environment value or log field.
	opts := append([]nats.Option(nil), t.opts...)
	opts = append(opts, nats.Name("transfer-settlement:"+r.Identity.SID+":"+r.Identity.NID), nats.Token(string(payload)), nats.ReconnectBufSize(-1), nats.FlusherTimeout(5*time.Second))
	s.nc, err = ConnectWithin(t.url, t.id, d, t.observer, opts...)
	return s.nc, err
}

// Close owns all attachments even when construction failed after allocation.
// The borrowed control connection is closed by the command after this join.
func (t *TransferTransport) Close() error {
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return err
	}
	t.mu.Lock()
	id := t.identity
	t.mu.Unlock()
	err = t.CloseData(id, d)
	t.settlement.mu.Lock()
	t.settlement.closed = true
	nc := t.settlement.nc
	t.settlement.mu.Unlock()
	if nc != nil {
		nc.Close()
	}
	return err
}
