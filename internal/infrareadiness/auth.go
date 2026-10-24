package infrareadiness

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats.go"
)

type Identity uint8

const (
	ActivatedMember  Identity = 1
	ProvisionedAgent Identity = 2
)

// AuthReceipt is produced by the normal queue handler, matched to the real
// CONNECT request and cid. A temporary non-queue probe cannot produce it.
type AuthReceipt struct {
	Identity        Identity
	PrincipalSHA256 infracontract.Digest
	ServerID        string
	CID             uint64
	Generation      uint64
	RequestSHA256   infracontract.Digest
	Queue           string
}

// WSSProof is produced by the TLS/HTTP101/INFO and physical ingress observers.
// P02/P14 own those observations; a URL or certificate alone cannot fill it.
type WSSProof struct {
	Attachment infracontract.Attachment
	SNI        string
	Pin        infracontract.Digest
	HTTP101    bool
}

type AuthEvidence struct {
	Identity     Identity
	ServerID     string
	CID          uint64
	Receipt      AuthReceipt
	PrivateInbox bool
	RPC          infracontract.Digest
	WSS          WSSProof
}

type AuthSource interface {
	Check(Server, Identity, infracontract.Deadline, infracontract.Stop) (AuthEvidence, error)
}

// OrdinaryAuth delegates the actual activated/provisioned dial, normal queue
// observation and allowed RPC to their existing owners. It adds no responder,
// bypass user or fallback credentials. Every returned connection is probe-owned
// and closed here, including failed constructor/receipt/inbox/RPC paths.
type OrdinaryAuth struct {
	Dial    func(Server, Identity, infracontract.Deadline, infracontract.Stop) (*nats.Conn, error)
	Receipt func(*nats.Conn, Identity, infracontract.Deadline, infracontract.Stop) (AuthReceipt, error)
	RPC     func(*nats.Conn, Identity, infracontract.Deadline, infracontract.Stop) (infracontract.Digest, error)
	WSS     func(Server, Identity, infracontract.Deadline, infracontract.Stop) (WSSProof, error)
}

var _ AuthSource = (*OrdinaryAuth)(nil)

// Check confirms SUB/PONG ordering before the ordinary RPC. The required normal
// queue receipt prevents a peer auth responder from masking a missing local one.
// origin: hot-update S9 plan §3.3 / T26
func (a *OrdinaryAuth) Check(s Server, identity Identity, deadline infracontract.Deadline, stop infracontract.Stop) (AuthEvidence, error) {
	if a == nil || a.Dial == nil || a.Receipt == nil || a.RPC == nil || a.WSS == nil || !validIdentity(identity) {
		return AuthEvidence{}, infracontract.ErrIncomplete
	}
	if err := admission(deadline, stop); err != nil {
		return AuthEvidence{}, err
	}
	nc, err := a.Dial(s, identity, deadline, stop)
	if nc != nil {
		defer nc.Close()
	}
	if err != nil || nc == nil || nc.ConnectedServerId() != s.ID {
		return AuthEvidence{}, infracontract.ErrIncomplete
	}
	cid, err := nc.GetClientID()
	if err != nil || cid == 0 {
		return AuthEvidence{}, infracontract.ErrIncomplete
	}
	receipt, err := a.Receipt(nc, identity, deadline, stop)
	if err != nil || receipt.Identity != identity || !validReceipt(receipt, s.ID, cid) {
		return AuthEvidence{}, infracontract.ErrIncomplete
	}
	// Request's actual private inbox is exercised by the RPC, while this SUB/PONG
	// proves the grant before sending it. Shared _INBOX fallback is insufficient.
	prefix := nc.Opts.InboxPrefix
	principal := hex.EncodeToString(receipt.PrincipalSHA256[:])
	if prefix != auth.InboxRoot+"."+principal[:8]+"."+principal[8:16] {
		return AuthEvidence{}, infracontract.ErrIncomplete
	}
	sub, err := nc.SubscribeSync(prefix + ".infra-readiness")
	if err != nil {
		return AuthEvidence{}, infracontract.ErrIncomplete
	}
	defer func() { _ = sub.Unsubscribe() }()
	ctx, finish, err := requestContext(deadline, stop)
	if err != nil {
		return AuthEvidence{}, err
	}
	err = nc.FlushWithContext(ctx)
	finish()
	if err != nil || nc.LastError() != nil {
		return AuthEvidence{}, infracontract.ErrIncomplete
	}
	rpc, err := a.RPC(nc, identity, deadline, stop)
	if err != nil || rpc == (infracontract.Digest{}) || nc.ConnectedServerId() != s.ID {
		return AuthEvidence{}, infracontract.ErrIncomplete
	}
	afterCID, err := nc.GetClientID()
	if err != nil || afterCID != cid {
		return AuthEvidence{}, infracontract.ErrIncomplete
	}
	wss, err := a.WSS(s, identity, deadline, stop)
	if err != nil || !validWSS(wss, s.ID) || admission(deadline, stop) != nil {
		return AuthEvidence{}, infracontract.ErrIncomplete
	}
	return AuthEvidence{Identity: identity, ServerID: s.ID, CID: cid, Receipt: receipt,
		PrivateInbox: true, RPC: rpc, WSS: wss}, nil
}

func validIdentity(identity Identity) bool {
	switch identity {
	case ActivatedMember, ProvisionedAgent:
		return true
	default:
		return false
	}
}

func validReceipt(r AuthReceipt, serverID string, cid uint64) bool {
	return validIdentity(r.Identity) && r.PrincipalSHA256 != (infracontract.Digest{}) && r.ServerID == serverID && r.CID == cid && r.CID != 0 && r.Generation != 0 &&
		r.RequestSHA256 != (infracontract.Digest{}) && r.Queue == "tether-authcallout"
}

func readAuth(source AuthSource, topology Topology, deadline infracontract.Deadline, stop infracontract.Stop) (infracontract.Digest, infracontract.Digest, error) {
	var auth []AuthEvidence
	var wss []WSSProof
	for _, s := range canonicalTopology(topology).Servers {
		if !s.Online {
			return infracontract.Digest{}, infracontract.Digest{}, infracontract.ErrIncomplete
		}
		for _, identity := range []Identity{ActivatedMember, ProvisionedAgent} {
			e, err := source.Check(s, identity, deadline, stop)
			if err != nil || e.Identity != identity || e.Receipt.Identity != identity || e.ServerID != s.ID || !validReceipt(e.Receipt, e.ServerID, e.CID) ||
				!e.PrivateInbox || e.RPC == (infracontract.Digest{}) || !validWSS(e.WSS, s.ID) {
				return infracontract.Digest{}, infracontract.Digest{}, infracontract.ErrIncomplete
			}
			auth = append(auth, e)
			wss = append(wss, e.WSS)
		}
	}
	return digest(auth), digest(wss), nil
}

func validWSS(w WSSProof, serverID string) bool {
	a, raw := w.Attachment, w.Attachment.Raw
	return w.HTTP101 && w.SNI != "" && w.Pin != (infracontract.Digest{}) && a.ServerID == serverID &&
		a.CID != 0 && a.Generation != 0 && a.HealthGeneration != 0 && strings.HasPrefix(a.URL, "wss://") &&
		validProcess(a.Ingress) && validProcess(raw.Owner) && raw.NetNS != 0 && raw.Device != 0 &&
		raw.Inode != 0 && raw.Cookie != 0 && raw.Local != "" && raw.Remote != ""
}

// The frozen public boundary has Deadline/Stop instead of a caller context. The
// private request context has one cancellation watcher, always cancelled/joined;
// the caller's business NC and its data owners never inherit this context.
func requestContext(deadline infracontract.Deadline, stop infracontract.Stop) (context.Context, func(), error) {
	if err := admission(deadline, stop); err != nil {
		return nil, nil, err
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), remaining) // ctx-none: frozen Deadline/Stop request boundary has no caller context.
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, func() { cancel(); <-done }, nil
}
