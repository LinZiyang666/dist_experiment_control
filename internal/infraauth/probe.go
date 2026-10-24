package infraauth

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// Identity is already activated/provisioned. No PIN or temporary unactivated
// identity is accepted by this probe; the normal handler decides ordinary ACLs.
type Identity struct {
	Name        string
	PublicKey   string
	Sign        nats.SignatureHandler
	Options     func() []nats.Option
	options     []nats.Option
	RPCSubject  string
	RPCRequest  []byte
	RPCResponse []byte
}

type ProbeReceipt struct {
	Generation    uint64
	ServerID      string
	CID           uint64
	RequestDigest infracontract.Digest
	ReplyBootNS   int64
	PONGBootNS    int64
	RPCBootNS     int64
}

func cloneIdentity(identity Identity) Identity {
	if identity.Options != nil {
		identity.options = append([]nats.Option(nil), identity.Options()...)
	}
	identity.Options = nil
	identity.RPCRequest = append([]byte(nil), identity.RPCRequest...)
	identity.RPCResponse = append([]byte(nil), identity.RPCResponse...)
	return identity
}

func validIdentity(identity Identity, agent bool) bool {
	parts := strings.Split(identity.Name, ":")
	validName := len(parts) == 2 && parts[0] == "tether-cli" && proto.ValidateSID(parts[1]) == nil
	if agent {
		validName = len(parts) == 3 && parts[0] == "tether-agent" && proto.ValidateSID(parts[1]) == nil && proto.ValidateNID(parts[2]) == nil
	}
	return validName && nkeys.IsValidPublicUserKey(identity.PublicKey) && identity.Sign != nil && identity.RPCSubject != "" && len(identity.RPCResponse) != 0
}

func guardedHandler(g *generation, handle nats.MsgHandler) nats.MsgHandler {
	// Add and the fence decision share a mutex. Wait starts only after the
	// fence, so a queued callback cannot race a zero WaitGroup into a false join.
	return func(msg *nats.Msg) {
		g.mu.Lock()
		if g.fenced {
			g.mu.Unlock()
			return
		}
		g.active.Add(1)
		g.mu.Unlock()
		defer g.active.Done()
		handle(msg)
	}
}

// The signed request/response witness is captured inside the normal queue
// callback when the server chose this member, or — when it chose a routed
// peer — by the SAME callout owner deciding the local server's captured
// request while this queue is still the acknowledged member (authenticate).
// CONNECT succeeding elsewhere, a static grant, or S6's temporary observer
// cannot fill these receipts. JWT decode validates signatures, while the
// explicit checks bind the issuer, ephemeral user, name and local server.
// origin: hot-update S9 plan §4.4 / T39
func (o *Owner) normalHandler(g *generation) nats.MsgHandler {
	return guardedHandler(g, func(msg *nats.Msg) {
		// Marked before deciding: when this member answers a probe CONNECT the
		// mark precedes Respond, and so precedes the server accepting that
		// CONNECT; the probe never re-decides a request its own queue answered.
		markTaken(g, msg.Data)
		response, err := o.config.HandleAuth(string(msg.Data))
		if err != nil || msg.Reply == "" {
			return
		}
		// Never hold the gate mutex across Respond: it can wait on nc.mu and a
		// blocked raw write. Fence must be able to cancel that transport first.
		if msg.Respond([]byte(response)) != nil {
			return
		}
		// Reply witnesses follow the actual delivery and precede the probe
		// witness below, so a probe released by it never outruns them.
		if o.config.Replied != nil {
			o.config.Replied(string(msg.Data), response)
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.fenced {
			return
		}
		o.observeResponse(g, string(msg.Data), response)
	})
}

func markTaken(g *generation, data []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if w := g.witness; w != nil && len(w.taken) < witnessEntries {
		w.taken[sha256.Sum256(data)] = true
	}
}

// armWitness installs the probe's non-responding capture on the auth NC and
// waits for its PONG, so the server holds the interest before the probe's
// CONNECT makes it publish the callout request.
// origin: hot-update S9 P22 resume — design finding (callout queue selection)
func (o *Owner) armWitness(g *generation, identity Identity, deadline infracontract.Deadline) (*probeWitness, error) {
	w := &probeWitness{identity: identity, captured: make(map[uint64]string), taken: make(map[infracontract.Digest]bool), arrived: make(chan struct{})}
	g.mu.Lock()
	if g.fenced || g.witness != nil || g.auth == nil {
		g.mu.Unlock()
		return nil, ErrFenced
	}
	g.witness = w
	serverID := g.receipt.ServerID
	g.mu.Unlock()
	sub, err := g.auth.Subscribe(authSubject, guardedHandler(g, func(msg *nats.Msg) { capture(g, w, serverID, msg.Data) }))
	if err != nil {
		disarmWitness(g, w)
		return nil, ErrBinding
	}
	w.sub = sub
	if err := flush(g, g.auth, deadline); err != nil {
		disarmWitness(g, w)
		return nil, err
	}
	return w, nil
}

func disarmWitness(g *generation, w *probeWitness) {
	g.mu.Lock()
	if g.witness == w {
		g.witness = nil
	}
	w.captured, w.taken = nil, nil
	g.mu.Unlock()
	if w.sub != nil {
		_ = w.sub.Unsubscribe()
	}
}

// capture keeps only the local server's own signed request for this probe's
// identity. A peer server's request, another client's, or one signed by any
// other issuer is ignored; nothing here ever responds.
func capture(g *generation, w *probeWitness, serverID string, data []byte) {
	req, err := jwt.DecodeAuthorizationRequestClaims(string(data))
	if err != nil || req.Issuer != serverID || req.Server.ID != serverID || req.ClientInformation.ID == 0 ||
		req.ConnectOptions.Nkey != w.identity.PublicKey || req.ConnectOptions.Name != w.identity.Name {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fenced || g.witness != w || len(w.captured) >= witnessEntries {
		return
	}
	w.captured[req.ClientInformation.ID] = string(data)
	close(w.arrived)
	w.arrived = make(chan struct{})
}

// normalQueueBound is the deterministic half of the local proof: this
// generation's normal queue SUB went out on the local-only auth NC before the
// PONG recorded as AuthPONG, the server answered it with no -ERR (nats.go
// keeps any permissions violation as LastError), and it is still subscribed.
// A peer's answer can neither create nor replace any of these facts.
func normalQueueBound(g *generation) error {
	g.mu.Lock()
	queue, nc, serverID, pong := g.normalQueue, g.auth, g.receipt.ServerID, g.receipt.AuthPONG
	g.mu.Unlock()
	if queue == nil || nc == nil || pong == 0 || !queue.IsValid() || queue.Subject != authSubject || queue.Queue != authQueue ||
		nc.LastError() != nil || !nc.IsConnected() || nc.ConnectedServerId() != serverID {
		return ErrNormalQueue
	}
	return nil
}

// awaitCapture waits for the local server's request for exactly this CID.
// Delivery to the armed witness is guaranteed by subject interest, so this is
// bounded by the probe deadline but never depends on the queue's choice.
func awaitCapture(g *generation, w *probeWitness, cid uint64, deadline infracontract.Deadline) (string, bool, error) {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return "", false, err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	for {
		g.mu.Lock()
		request, ok := w.captured[cid]
		taken := ok && w.taken[sha256.Sum256([]byte(request))]
		arrived := w.arrived
		g.mu.Unlock()
		if ok {
			return request, taken, nil
		}
		select {
		case <-arrived:
		case <-g.lost:
			return "", false, ErrFenced
		case <-timer.C:
			return "", false, ErrNormalQueue
		}
	}
}

// decide runs the SAME callout owner on a captured request a routed peer
// answered. It is accounted like a queue callback, so Close joins it, and its
// result is only observed: it is never sent and never reported as a reply.
func (o *Owner) decide(g *generation, request string, deadline infracontract.Deadline) error {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return err
	}
	g.mu.Lock()
	if g.fenced {
		g.mu.Unlock()
		return ErrFenced
	}
	g.active.Add(1)
	g.mu.Unlock()
	decided := make(chan bool, 1)
	go func() {
		defer g.active.Done()
		response, err := o.config.HandleAuth(request)
		recorded := false
		if err == nil {
			g.mu.Lock()
			recorded = !g.fenced && o.observeResponse(g, request, response)
			g.mu.Unlock()
		}
		decided <- recorded
	}()
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case recorded := <-decided:
		// A refusal, a wrongly bound grant or a failed decision is the local
		// owner disagreeing with the peer that let this CONNECT in.
		if !recorded {
			return ErrNormalQueue
		}
		return nil
	case <-g.lost:
		return ErrFenced
	case <-timer.C:
		return ErrNormalQueue
	}
}

// observeResponse reports whether it recorded a probe witness. The caller
// holds g.mu.
func (o *Owner) observeResponse(g *generation, request, response string) bool {
	// Checking the server-signed request independently matters even when the
	// handler already decoded it: the witness must describe THIS local queue's
	// generation, not a peer request routed to an otherwise healthy handler.
	req, err := jwt.DecodeAuthorizationRequestClaims(request)
	if err != nil || req.Issuer != g.receipt.ServerID || req.Server.ID != g.receipt.ServerID || req.ClientInformation.ID == 0 {
		return false
	}
	resp, err := jwt.DecodeAuthorizationResponseClaims(response)
	if err != nil || resp.Error != "" || resp.Issuer != o.config.AccountPublicKey || resp.Subject != req.UserNkey || resp.Audience != req.Server.ID {
		return false
	}
	user, err := jwt.DecodeUserClaims(resp.Jwt)
	// The wrapper and enclosed user grant are independently signed records.
	// A valid response wrapper does not authorize a differently issued user or
	// another ephemeral CONNECT user embedded inside it.
	if err != nil || user.Issuer != o.config.AccountPublicKey || user.Subject != req.UserNkey {
		return false
	}
	recorded := false
	for index, identity := range [2]Identity{o.config.Member, o.config.Agent} {
		// These are the provisioned identities supplied by the real binding.
		// Successful unactivated or unrelated clients cannot populate either
		// required slot, even if they use the same account and local queue.
		if req.ConnectOptions.Nkey != identity.PublicKey || req.ConnectOptions.Name != identity.Name {
			continue
		}
		now, err := controlbudget.Now()
		if err != nil {
			return false
		}
		g.responses[index] = ProbeReceipt{Generation: g.receipt.Generation, ServerID: req.Server.ID, CID: req.ClientInformation.ID, RequestDigest: sha256.Sum256([]byte(request)), ReplyBootNS: now}
		recorded = true
		select {
		case <-g.responseReady[index]:
		default:
			close(g.responseReady[index])
		}
	}
	return recorded
}

// authenticate proves one ordinary identity against the LOCAL owner without
// depending on which queue member the server picks. A peer answer only lets
// the CONNECT in; the local proof is always this generation's acknowledged
// normal queue plus this owner's own decision of the local server's request
// for exactly this CID — the queue's answer, or the same callout owner run on
// the witness capture. So a peer can neither sign a missing or broken local
// owner (never passes on a peer's answer) nor fail a healthy one (never fails
// because a peer answered).
// origin: hot-update S9 plan §4.4 / T39; S9 P22 resume — design finding
func (o *Owner) authenticate(g *generation, identity Identity, index int, deadline infracontract.Deadline) (*nats.Conn, error) {
	if err := normalQueueBound(g); err != nil {
		return nil, err
	}
	w, err := o.armWitness(g, identity, deadline)
	if err != nil {
		return nil, err
	}
	disarmed := false
	defer func() {
		if !disarmed {
			disarmWitness(g, w)
		}
	}()
	opts := append([]nats.Option(nil), o.options...)
	opts = append(opts, identity.options...)
	// The private inbox and current identity are mandatory even if an older
	// credential builder appended shared-inbox or static-broker options.
	// Both inbox halves come from the one pairing helper (prefix + CONNECT
	// marker), appended last so they still override any earlier option.
	opts = append(opts, nats.Nkey(identity.PublicKey, identity.Sign), nats.Token(""))
	opts = append(opts, natsinbox.InboxConnectOptions(identity.PublicKey)...)
	nc, err := o.connect(g, opts, identity.Name, false, deadline)
	if err != nil {
		return nil, err
	}
	if err := flush(g, nc, deadline); err != nil {
		return nil, err
	}
	serverID, cid, err := connectionIdentity(nc)
	if err != nil {
		return nil, err
	}
	request, taken, err := awaitCapture(g, w, cid, deadline)
	if err != nil {
		return nil, err
	}
	// Re-check after the CONNECT: the queue that is credited must still be the
	// acknowledged member, not one removed while a peer answered.
	if err := normalQueueBound(g); err != nil {
		return nil, err
	}
	if !taken {
		// The server chose a routed peer. Its answer admitted the CONNECT, but
		// the local proof is this owner deciding the same signed request.
		if err := o.decide(g, request, deadline); err != nil {
			return nil, err
		}
	}
	// The witness leaves the server before readiness: an armed plain SUB is
	// delivered every callout in the cluster.
	disarmWitness(g, w)
	disarmed = true
	if err := flush(g, g.auth, deadline); err != nil {
		return nil, err
	}
	// CONNECT can finish before its queue callback is scheduled after Respond.
	// Wait for the actual reply witness without accepting a peer's successful
	// CONNECT as evidence of this owner's normal queue.
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-g.responseReady[index]:
	case <-g.lost:
		return nil, ErrFenced
	case <-timer.C:
		return nil, ErrNormalQueue
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	proof := g.responses[index]
	// Exact CID excludes a successful auxiliary/peer response accompanied by a
	// stale local receipt for the same key. Same URL, key or serverID alone is
	// deliberately insufficient: CONNECT makes a new ephemeral incarnation.
	if g.fenced || proof.Generation != g.receipt.Generation || proof.ServerID != serverID || serverID != g.receipt.ServerID || proof.CID != cid || proof.ReplyBootNS <= g.receipt.AuthPONG || proof.RequestDigest == (infracontract.Digest{}) {
		return nil, ErrNormalQueue
	}
	now, err := controlbudget.Now()
	if err != nil {
		return nil, err
	}
	proof.PONGBootNS = now
	g.receipt.Probes[index] = proof
	return nc, nil
}

// No special probe RPC responder is installed. These requests use the actual
// business descriptor and the activated member/agent's ordinary private inbox.
// A denied publish, private SUB, route or wrong response prevents control-ready.
// origin: hot-update S9 plan §4.4 / T38 / T39
func probeRPC(g *generation, nc *nats.Conn, identity Identity, index int, deadline infracontract.Deadline) error {
	if err := live(g, deadline); err != nil {
		return err
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return err
	}
	response, err := nc.Request(identity.RPCSubject, identity.RPCRequest, remaining)
	if err != nil || !bytes.Equal(response.Data, identity.RPCResponse) {
		return ErrProbe
	}
	if err := flush(g, nc, deadline); err != nil {
		return err
	}
	now, err := controlbudget.Now()
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.receipt.Probes[index].RPCBootNS = now
	g.mu.Unlock()
	return nil
}
