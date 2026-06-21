package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/proxydial"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// completionDialTimeout caps the NATS dial portion of a completion call.
// The outer CompletionBudget (1s) further bounds the whole flow; using
// a separate dial cap keeps RPC time available even on a slow link.
const completionDialTimeout = 750 * time.Millisecond

// ReadIdentityIfExists is a read-only sibling of EnsureIdentity used by
// the completion path. It returns (nil, nil) if no key file exists, so
// tabbing on a fresh machine produces no side effects — no nkey is
// generated, no directory is written. Any other read error returns an
// error.
//
// Production RunE paths should keep using EnsureIdentity (which mints
// on miss); only the completion subprocess wants this read-only mode.
func ReadIdentityIfExists(home string) (*Identity, error) {
	keyPath := filepath.Join(home, "keys", "default.nk")
	seed, err := os.ReadFile(keyPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("identity: read: %w", err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		return nil, fmt.Errorf("identity: derive pub: %w", err)
	}
	fp, err := auth.FingerprintFromActor(pub)
	if err != nil {
		return nil, fmt.Errorf("identity: derive fp: %w", err)
	}
	return &Identity{Seed: seed, PublicKey: pub, Fingerprint: fp}, nil
}

// NewCompletionContext resolves the read-side fields needed by the
// completion helpers. It does not connect to NATS and does not create
// keys; it only reads existing state on disk + environment.
//
// `home` and `natsURLFlag` come from `flagsAtCompletionTime(cmd)` in
// the cobra wiring. `flagChanged` is `cmd.Flags().Changed("nats-url")`
// (or the equivalent for `--broker` / `--socket`).
func NewCompletionContext(home, natsURLFlag string, flagChanged bool) CompletionContext {
	id, _ := ReadIdentityIfExists(home)
	cctx := CompletionContext{
		Home:    home,
		NATSURL: ResolveNATSURLFromHome(natsURLFlag, flagChanged, home),
		SID:     ReadCurrentSession(home),
	}
	if id != nil {
		cctx.ActorPubKey = id.PublicKey
	}
	return cctx
}

// natsTransport is the production Transport. Each List* method dials
// its own short-lived connection with the connection-Name that matches
// the broker's auth_callout expectation for that call (unactivated for
// session list, activated-member for node/ps). A single completion
// subprocess only calls one helper, so the per-call dial cost is paid
// at most once.
//
// Why not reuse one connection across helpers: ListSessions must use
// CtlNameUnactivated (session.list is in the unactivated permission
// template) so that a stale ~/.tether/current_session pointing at a
// deleted/inaccessible sid does NOT block `login -s <TAB>` from
// listing the sessions the user actually CAN see. ListNodes/ListPorts
// need the activated template (ps.req / node.list.req). One connection
// can only carry one name; honoring both means dialing twice or
// dialing once per helper. Per-helper is simpler.
type natsTransport struct {
	cctx    CompletionContext
	id      *Identity
	connect func(name string) (*nats.Conn, error) // overridable for tests; nil → real dial

	closed bool
}

// NewCompletionTransport builds the production NATS transport. If
// natsURL is empty or no identity exists, returns a no-op transport
// that yields zero candidates from every helper — tab on a fresh /
// logged-out machine is silent.
func NewCompletionTransport(home, natsURLFlag string, flagChanged bool) Transport {
	cctx := NewCompletionContext(home, natsURLFlag, flagChanged)
	if cctx.NATSURL == "" || cctx.ActorPubKey == "" {
		return noopTransport{}
	}
	id, err := ReadIdentityIfExists(home)
	if err != nil || id == nil {
		return noopTransport{}
	}
	return &natsTransport{cctx: cctx, id: id}
}

// dial opens a one-shot NATS connection with completion-specific
// options: short dial cap, no reconnect, no retry. Caller is
// responsible for Close()-ing the returned conn.
//
// `name` is the connection-Name template (CtlNameUnactivated for
// session.list, CtlNameForSession(sid) for ps/node.list). See type doc.
func (t *natsTransport) dial(ctx context.Context, name string) (*nats.Conn, error) {
	if t.closed {
		return nil, errors.New("transport closed")
	}
	connectFn := t.connect
	if connectFn == nil {
		connectFn = func(n string) (*nats.Conn, error) {
			seed := append([]byte(nil), t.id.Seed...)
			pubKey := t.id.PublicKey
			sigCB := func(nonce []byte) ([]byte, error) {
				kp, err := nkeys.FromSeed(seed)
				if err != nil {
					return nil, err
				}
				defer kp.Wipe()
				return kp.Sign(nonce)
			}
			opts := []nats.Option{
				nats.Name(n),
				nats.Timeout(completionDialTimeout),
				nats.RetryOnFailedConnect(false),
				nats.MaxReconnects(0),
				nats.NoEcho(),
				nats.Nkey(pubKey, sigCB),
			}
			// Proxy-aware dial; pass the completion budget so a ctx-cancel can't
			// leave a dial goroutine lingering past the tab-completion deadline.
			popts, err := proxydial.Options(proxydial.OSEnv, completionDialTimeout)
			if err != nil {
				return nil, err
			}
			opts = append(opts, popts...)
			return nats.Connect(t.cctx.NATSURL, opts...)
		}
	}

	// Run dial in a goroutine so we can honor ctx cancellation: nats.Connect
	// doesn't natively take a context, but its internal Timeout option
	// already bounds dial. We still cancel on ctx so the helper returns
	// promptly if the outer 1s budget expires before dial completes.
	type result struct {
		nc  *nats.Conn
		err error
	}
	ch := make(chan result, 1)
	go func() {
		nc, err := connectFn(name)
		ch <- result{nc: nc, err: err}
	}()
	select {
	case r := <-ch:
		return r.nc, r.err
	case <-ctx.Done():
		// Spawn a cleanup goroutine to close the connection if dial finishes
		// after our budget. Avoids a leak when the helper times out but the
		// dial succeeds a moment later.
		go func() {
			r := <-ch
			if r.nc != nil {
				r.nc.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

func (t *natsTransport) Close() {
	// Per-call dial; nothing held at transport level. Idempotent.
	t.closed = true
}

func (t *natsTransport) ListNodes(ctx context.Context) ([]NodeInfo, error) {
	if t.cctx.SID == "" {
		return nil, nil
	}
	// ps.req / node.list.req are gated by the activated-member JWT
	// template, so the connection must carry CtlNameForSession(sid).
	nc, err := t.dial(ctx, CtlNameForSession(t.cctx.SID))
	if err != nil {
		return nil, err
	}
	defer nc.Close()
	body, _ := json.Marshal(proto.NodeListReq{})
	subj := proto.SubjCtrlNodeList(t.id.PublicKey, t.cctx.SID)
	msg, err := nc.RequestWithContext(ctx, subj, body)
	if err != nil {
		return nil, err
	}
	var resp proto.NodeListResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, err
	}
	if resp.Code != "" {
		return nil, fmt.Errorf("%s: %s", resp.Code, resp.Error)
	}
	out := make([]NodeInfo, 0, len(resp.Nodes))
	for _, n := range resp.Nodes {
		out = append(out, NodeInfo{NID: n.NID, Status: n.Status})
	}
	return out, nil
}

func (t *natsTransport) ListSessions(ctx context.Context) ([]SessionInfo, error) {
	// session.list is in the unactivated permission template; using
	// CtlNameUnactivated here means a stale ~/.tether/current_session
	// pointing at a deleted / DELETING / non-member sid does NOT cause
	// auth_callout to reject the CONNECT — the helper still returns
	// the sessions the user genuinely can see / owns. Implementation
	// review High #1.
	nc, err := t.dial(ctx, CtlNameUnactivated)
	if err != nil {
		return nil, err
	}
	defer nc.Close()
	body, _ := json.Marshal(proto.SessionListReq{})
	subj := proto.SubjCtrlSessionList(t.id.PublicKey)
	msg, err := nc.RequestWithContext(ctx, subj, body)
	if err != nil {
		return nil, err
	}
	var resp proto.SessionListResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, err
	}
	if resp.Code != "" {
		return nil, fmt.Errorf("%s: %s", resp.Code, resp.Error)
	}
	out := make([]SessionInfo, 0, len(resp.Sessions))
	for _, s := range resp.Sessions {
		out = append(out, SessionInfo{SID: s.SID, State: s.State, IsOwner: s.IsOwner})
	}
	return out, nil
}

func (t *natsTransport) ListPorts(ctx context.Context) ([]PortInfo, error) {
	if t.cctx.SID == "" {
		return nil, nil
	}
	nc, err := t.dial(ctx, CtlNameForSession(t.cctx.SID))
	if err != nil {
		return nil, err
	}
	defer nc.Close()
	body, _ := json.Marshal(proto.PsReq{})
	subj := proto.SubjCtrlPs(t.id.PublicKey, t.cctx.SID)
	msg, err := nc.RequestWithContext(ctx, subj, body)
	if err != nil {
		return nil, err
	}
	var resp proto.PsResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, err
	}
	if resp.Code != "" {
		return nil, fmt.Errorf("%s: %s", resp.Code, resp.Error)
	}
	out := make([]PortInfo, 0, len(resp.Ports))
	for _, p := range resp.Ports {
		out = append(out, PortInfo{Name: p.Name, NID: p.NID, State: p.State})
	}
	return out, nil
}

// NewTestNATSTransport builds a Transport that dials the given NATS
// URL anonymously (no nkey signing). The anonymous-NATS test harness
// used by test/cli_e2e doesn't run auth_callout, so we skip nkey
// options to avoid the production sigCB path. The connection-Name
// selection (CtlNameUnactivated vs CtlNameForSession) is still
// exercised — that's the whole point of the e2e regression test for
// the High #1 stale-SID fix.
//
// Production code MUST NOT use this; it bypasses identity loading.
func NewTestNATSTransport(natsURL, actorPub, sid string) Transport {
	return &natsTransport{
		cctx: CompletionContext{NATSURL: natsURL, ActorPubKey: actorPub, SID: sid},
		id:   &Identity{PublicKey: actorPub, Seed: []byte("test-only-seed-ignored-by-anon-nats")},
		connect: func(name string) (*nats.Conn, error) {
			return nats.Connect(natsURL,
				nats.Name(name),
				nats.Timeout(completionDialTimeout),
				nats.RetryOnFailedConnect(false),
				nats.MaxReconnects(0),
				nats.NoEcho(),
			)
		},
	}
}

// noopTransport returns empty candidates from every helper. Used when
// the operator has no identity / no broker URL / no active session, so
// `<TAB>` is silent and never blocks.
type noopTransport struct{}

func (noopTransport) ListNodes(context.Context) ([]NodeInfo, error)       { return nil, nil }
func (noopTransport) ListSessions(context.Context) ([]SessionInfo, error) { return nil, nil }
func (noopTransport) ListPorts(context.Context) ([]PortInfo, error)       { return nil, nil }
func (noopTransport) Close()                                              {}

// adminCompletionClient builds an adminsock.Client with the
// CompletionBudget timeout, so a wedged broker socket (accept but no
// reply) doesn't push tab latency past 1 s. Without this it would fall
// back to adminsock's default 5 s. Implementation review Medium #2.
func adminCompletionClient(socketPath string) *adminsock.Client {
	return &adminsock.Client{Path: socketPath, Timeout: CompletionBudget}
}

// adminListSessions queries the broker's local admin socket and returns
// the SIDs of every session it knows about. Used by `tether admin audit
// <TAB>` and `tether admin evict <TAB>` (first positional).
func adminListSessions(socketPath string) ([]string, error) {
	c := adminCompletionClient(socketPath)
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpSessions})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	out := make([]string, 0, len(resp.Sessions))
	for _, s := range resp.Sessions {
		out = append(out, s.SID)
	}
	return out, nil
}

// adminListNodes queries the broker's local admin socket and returns
// node ids. If sid is non-empty, results are filtered to that session.
func adminListNodes(socketPath, sid string) ([]string, error) {
	c := adminCompletionClient(socketPath)
	resp, err := c.Call(adminsock.Request{Op: adminsock.OpNodes})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	out := make([]string, 0, len(resp.Nodes))
	for _, n := range resp.Nodes {
		if sid != "" && n.SID != sid {
			continue
		}
		out = append(out, n.NID)
	}
	return out, nil
}
