package cluster

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterio"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// tlsStreamLayer is a raft.StreamLayer (net.Listener + Dial) over mutual-TLS TCP.
//
// Peer trust = "the leaf chains to the cluster CA" (a service-mesh trust model),
// NOT hostname match: raft peer addresses are private IP:port and carry no DNS
// SANs, so the standard hostname check would reject every legitimate peer. We
// therefore skip hostname verification on the client side and verify the chain to
// the cluster CA ourselves, while the server side uses the standard
// RequireAndVerifyClientCert path (client certs are never hostname-checked).
//
// This is the D3 bar. The stronger node-identity nkey-leaf pinning (so a leaked
// cluster-CA key alone cannot join a route) is D7 join-PoP (§18.1 / d3-plan R5).
// There is NO raft.NewTLSTransport in hashicorp/raft v1.7.3 — a custom
// StreamLayer fed to NewNetworkTransportWithConfig is the supported path.
type tlsStreamLayer struct {
	ln        net.Listener
	clientTLS *tls.Config
	tracker   *clusterio.TransportTracker
	network   *raft.NetworkTransport
	closeOnce sync.Once
	closeErr  error
}

func (s *tlsStreamLayer) Accept() (net.Conn, error) {
	return s.ln.Accept()
}
func (s *tlsStreamLayer) Close() error {
	s.closeOnce.Do(func() {
		transportStreams.Delete(s.network)
		s.closeErr = errors.Join(s.ln.Close(), s.tracker.Stop())
	})
	return s.closeErr
}
func (s *tlsStreamLayer) Addr() net.Addr { return s.ln.Addr() }

func (s *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := &net.Dialer{Timeout: timeout}
	raw, err := d.Dial("tcp", string(address))
	if err != nil {
		return nil, err
	}
	tracked, err := s.tracker.Track(raw)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	c := tls.Client(tracked, s.clientTLS)
	if timeout > 0 {
		_ = c.SetDeadline(time.Now().Add(timeout))
	}
	// ctx-none: Raft supplies the socket deadline; no caller context owns this connection.
	if err := c.HandshakeContext(context.Background()); err != nil {
		_ = c.Close()
		return nil, err
	}
	if err := c.SetDeadline(time.Time{}); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// MTLSTransportConfig configures NewMTLSTransport. CACert is the cluster CA pool
// (both the trust anchor for verifying peers and — by design — the only thing
// that must match). Leaf is this node's cluster-CA-signed leaf + private key.
type MTLSTransportConfig struct {
	BindAddr string         // listen address, e.g. "127.0.0.1:7400" (":0" picks a free port in tests)
	CACert   *x509.CertPool // cluster CA trust anchor (required)
	Leaf     tls.Certificate
	MaxPool  int           // 0 => 3
	Timeout  time.Duration // 0 => 10s
}

// clusterTLSConfigs builds the (server, client) mutual-TLS configs for tether's OWN raft
// TCP transport, anchored on the cluster CA. Both reject a leaf that does not chain to the CA:
// the server via the standard RequireAndVerifyClientCert path, the client via
// VerifyPeerCertificate. Hostname verification is skipped HERE because raft peers are dialed by
// IP:port and this transport's leaf need carry no SAN — a CN-only cert is fine for raft.
//
// NOTE (G1 #24): the SAME route-cert.pem is ALSO consumed by nats-server's cluster route mesh
// (internal/natscluster renders `cluster{ tls{ verify: true } }`), which does STANDARD Go x509
// verification and REQUIRES a subjectAltName matching the route-URL host (DNS: for a hostname
// route URL, IP: for an IP one). So a CN-only route leaf works for raft here yet breaks the nats
// route mesh (`x509: certificate relies on legacy Common Name field`) — the route/tunnel leaf
// MUST carry that SAN. See docs/cluster-runbook.md. Do NOT read the skip below as "no SAN needed".
func clusterTLSConfigs(cfg MTLSTransportConfig) (server, client *tls.Config, err error) {
	if cfg.CACert == nil {
		return nil, nil, fmt.Errorf("cluster: mTLS requires CACert")
	}
	if len(cfg.Leaf.Certificate) == 0 {
		return nil, nil, fmt.Errorf("cluster: mTLS requires a Leaf certificate")
	}
	verifyChainToCA := func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("cluster mTLS: peer presented no certificate")
		}
		leaf, perr := x509.ParseCertificate(rawCerts[0])
		if perr != nil {
			return fmt.Errorf("cluster mTLS: parse peer leaf: %w", perr)
		}
		inter := x509.NewCertPool()
		for _, der := range rawCerts[1:] {
			if c, e := x509.ParseCertificate(der); e == nil {
				inter.AddCert(c)
			}
		}
		if _, e := leaf.Verify(x509.VerifyOptions{Roots: cfg.CACert, Intermediates: inter}); e != nil {
			return fmt.Errorf("cluster mTLS: peer leaf does not chain to cluster CA: %w", e)
		}
		return nil
	}
	server = &tls.Config{
		Certificates: []tls.Certificate{cfg.Leaf},
		ClientCAs:    cfg.CACert,
		ClientAuth:   tls.RequireAndVerifyClientCert, // standard-path verify of the client chain to CA
		MinVersion:   tls.VersionTLS12,
	}
	client = &tls.Config{
		Certificates:          []tls.Certificate{cfg.Leaf},
		RootCAs:               cfg.CACert,
		InsecureSkipVerify:    true, // skip HOSTNAME only for THIS raft transport (IP:port peers, CN-only leaf ok); the nats route mesh reusing route-cert.pem still REQUIRES a SAN (#24, see doc comment above)
		VerifyPeerCertificate: verifyChainToCA,
		MinVersion:            tls.VersionTLS12,
	}
	return server, client, nil
}

// NewMTLSTransport builds a raft NetworkTransport whose StreamLayer is mutual-TLS
// over TCP, anchored on the cluster CA. The returned transport is injected at
// cluster.Config.Transport and MUST be closed by Node.Shutdown (raft does not
// reap an injected transport). A foreign-CA peer is rejected fail-closed in both
// directions (client: VerifyPeerCertificate; server: RequireAndVerifyClientCert).
func NewMTLSTransport(cfg MTLSTransportConfig) (*raft.NetworkTransport, error) {
	serverTLS, clientTLS, err := clusterTLSConfigs(cfg)
	if err != nil {
		return nil, err
	}

	rawListener, err := net.Listen("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: mTLS listen %q: %w", cfg.BindAddr, err)
	}
	tracker := clusterio.NewTracker()
	stream := &tlsStreamLayer{clientTLS: clientTLS, tracker: tracker}
	stream.ln = tls.NewListener(&trackedListener{Listener: rawListener, tracker: tracker}, serverTLS)

	maxPool := cfg.MaxPool
	if maxPool == 0 {
		maxPool = 3
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	network := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  stream,
		MaxPool: maxPool,
		Timeout: timeout,
		Logger:  hclog.NewNullLogger(), // discard raft-net chatter (D1 convention)
	})
	stream.network = network
	transportStreams.Store(network, stream)
	return network, nil
}

// Tracking sits below TLS, so a stalled handshake is cancellable as well as a
// stalled snapshot body. Wrapping tls.Conn after its handshake misses that seam.
// origin: hot-update S6 plan §3.2 / T16
type trackedListener struct {
	net.Listener
	tracker *clusterio.TransportTracker
}

func (l *trackedListener) Accept() (net.Conn, error) {
	raw, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	c, err := l.tracker.Track(raw)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	return c, nil
}

var transportStreams sync.Map // *raft.NetworkTransport -> *tlsStreamLayer

// imageNetworkTransport preserves the public network-transport constructor
// while putting one consumer boundary between decoded RPCs and Raft. The ticket
// travels on the exact reader submitted with that RPC, never on global FSM state.
// origin: hot-update S6 plan §3.4 / T16
type imageNetworkTransport struct {
	*raft.NetworkTransport
	tracker       *clusterio.TransportTracker
	stream        *tlsStreamLayer
	owner         *Node
	stage         func(*Node, *raft.InstallSnapshotRequest, clusterio.SnapshotTicket, io.Reader) (io.ReadCloser, error)
	consumer      chan raft.RPC
	stop          chan struct{}
	done          chan struct{}
	once          sync.Once
	startOnce     sync.Once
	workers       sync.WaitGroup
	rpcID         atomic.Uint64
	fenced        atomic.Bool
	closeErr      error
	stageMu       sync.Mutex
	unknownStages []io.ReadCloser
}

type snapshotTicketReader struct {
	io.ReadCloser
	ticket clusterio.SnapshotTicket
}

func (r *snapshotTicketReader) Ticket() clusterio.SnapshotTicket { return r.ticket }

func wrapImageTransport(transport raft.Transport) raft.Transport {
	wrapped := wrapNodeImageTransport(transport, nil)
	if network, ok := wrapped.(*imageNetworkTransport); ok {
		network.start()
	}
	return wrapped
}

func wrapNodeImageTransport(transport raft.Transport, owner *Node) raft.Transport {
	network, ok := transport.(*raft.NetworkTransport)
	if !ok {
		return transport
	}
	v, ok := transportStreams.Load(network)
	if !ok {
		return transport
	}
	stream, ok := v.(*tlsStreamLayer)
	// A corrupt registry must never bypass tracked cancellation on the image path.
	// origin: hot-update S6 plan §3.2 / T16
	if !ok || stream == nil {
		panic("cluster: invalid registered transport stream")
	}
	t := &imageNetworkTransport{NetworkTransport: network, tracker: stream.tracker, stream: stream, owner: owner, stage: imageSnapshotStage, consumer: make(chan raft.RPC), stop: make(chan struct{}), done: make(chan struct{})}
	return t
}

func (t *imageNetworkTransport) start() { t.startOnce.Do(func() { go t.dispatch() }) }

type stagedRPCReader struct {
	io.ReadCloser
	input     io.ReadCloser
	transport *imageNetworkTransport
	once      sync.Once
	err       error
}

func (r *stagedRPCReader) Close() error {
	r.once.Do(func() {
		r.err = errors.Join(r.ReadCloser.Close(), r.input.Close())
		if r.err != nil {
			r.transport.stageMu.Lock()
			r.transport.unknownStages = append(r.transport.unknownStages, r.ReadCloser)
			r.transport.stageMu.Unlock()
			r.transport.tracker.NoteCloseError(r.err)
		}
	})
	return r.err
}

func (t *imageNetworkTransport) Consumer() <-chan raft.RPC { return t.consumer }

func (t *imageNetworkTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, req *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	if t.fenced.Load() {
		return clusterio.ErrFenced
	}
	reader, err := t.tracker.TrackReader(data)
	if err != nil {
		return err
	}
	err = t.NetworkTransport.InstallSnapshot(id, target, req, resp, reader)
	return errors.Join(err, reader.Close())
}
func (t *imageNetworkTransport) SetHeartbeatHandler(fn func(raft.RPC)) {
	t.NetworkTransport.SetHeartbeatHandler(func(rpc raft.RPC) {
		if t.fenced.Load() {
			rpc.Respond(nil, clusterio.ErrFenced)
			return
		}
		fn(rpc)
	})
}

func (t *imageNetworkTransport) dispatch() {
	defer close(t.done)
	for {
		select {
		case <-t.stop:
			return
		case rpc := <-t.NetworkTransport.Consumer():
			if t.fenced.Load() {
				rpc.Respond(nil, clusterio.ErrFenced)
				continue
			}
			if req, ok := rpc.Command.(*raft.InstallSnapshotRequest); ok {
				if req.Size <= 0 || req.LastLogIndex == 0 || req.LastLogTerm == 0 || req.ConfigurationIndex > req.LastLogIndex {
					rpc.Respond(nil, errors.New("cluster: invalid snapshot RPC metadata"))
					continue
				}
				reader, err := t.tracker.TrackReader(rpc.Reader)
				if err != nil {
					rpc.Respond(nil, err)
					continue
				}
				digest := sha256.Sum256(req.Configuration)
				ticket := clusterio.SnapshotTicket{RPCID: t.rpcID.Add(1), Index: req.LastLogIndex, Term: req.LastLogTerm, ConfigIndex: req.ConfigurationIndex, ConfigDigest: hex.EncodeToString(digest[:]), PayloadBytes: req.Size}
				if t.owner != nil {
					// Raft stages to a different file before Restore. The P05 owner
					// must validate/bind the entire payload before this RPC reaches
					// Raft, otherwise rejection could already have changed its term.
					if t.stage == nil {
						_ = reader.Close()
						rpc.Respond(nil, errors.New("cluster: snapshot validation unavailable (S6 P05)"))
						continue
					}
					staged, stageErr := t.stage(t.owner, req, ticket, reader)
					if staged != nil {
						reader = &stagedRPCReader{ReadCloser: staged, input: reader, transport: t}
					}
					if stageErr != nil || staged == nil {
						closeErr := reader.Close()
						rpc.Respond(nil, errors.Join(errors.New("cluster: snapshot staging refused"), stageErr, closeErr))
						continue
					}
				}
				wrapped := &snapshotTicketReader{ReadCloser: reader, ticket: ticket}
				original := rpc
				rpc.Reader = wrapped
				response := make(chan raft.RPCResponse, 1)
				rpc.RespChan = response
				t.workers.Add(1)
				go func() {
					defer t.workers.Done()
					// The tracked reader's Close retains its owner and records any
					// uncertainty in the tracker; the response cannot clear it.
					// origin: hot-update S6 plan §3.2 / T16
					defer func() { _ = wrapped.Close() }()
					select {
					case reply := <-response:
						original.Respond(reply.Response, reply.Error)
					case <-t.stop:
						original.Respond(nil, raft.ErrTransportShutdown)
					}
				}()
			}
			select {
			case t.consumer <- rpc:
			case <-t.stop:
				rpc.Respond(nil, raft.ErrTransportShutdown)
				return
			}
		}
	}
}

func (t *imageNetworkTransport) fence(deadline controlbudget.Deadline) error {
	if _, err := clusterio.Remaining(deadline); err != nil {
		return err
	}
	err := t.cancelIO()
	if err != nil {
		return err
	}
	if t.owner != nil {
		if err := joinImageSnapshotWork(t.owner, deadline); err != nil {
			return err
		}
	}
	return t.tracker.Join(deadline)
}

func (t *imageNetworkTransport) cancelIO() error {
	t.fenced.Store(true)
	err := t.tracker.Stop()
	t.CloseStreams() // returns idle outgoing pooled owners too
	return err
}

func (t *imageNetworkTransport) Close() error {
	t.once.Do(func() {
		t.fenced.Store(true)
		stopErr := t.tracker.Stop()
		close(t.stop)
		t.start() // constructor failures may precede Consumer activation
		t.CloseStreams()
		closeErr := t.NetworkTransport.Close()
		<-t.done
		t.workers.Wait()
		t.closeErr = errors.Join(stopErr, closeErr, t.stream.closeErr, t.tracker.Wait())
	})
	return t.closeErr
}

// StageSnapshotRPC closes the tracked payload reader after bounded staging. P04
// derives ticket from the actual request (including SHA256(Configuration)) and
// forwards the returned reader with that unchanged request. The returned owner
// MUST remain open until rpc.Respond completes, even though Raft reopens a copy.
// One outstanding binding prevents identical payloads with conflicting config
// metadata from aliasing. P04 must reject a request/ticket mismatch before Raft
// sees it (Raft may advance its election term before it calls FSM.Restore).
func StageSnapshotRPC(n *Node, ticket clusterio.SnapshotTicket, source io.ReadCloser) (io.ReadCloser, error) {
	defer func() { _ = source.Close() }()
	if err := validateSnapshotTicket(ticket); err != nil {
		return nil, err
	}
	RequireSnapshotTickets(n)
	f := n.fsm
	ctx, finish, err := beginSnapshotWork(f, source)
	if err != nil {
		return nil, err
	}
	release := true
	defer func() {
		if release {
			finish()
		}
	}()
	tmp, err := os.CreateTemp(f.tmpDir, "rpc-snapshot-*.db")
	if err != nil {
		return nil, fmt.Errorf("cluster: RPC snapshot stage: %w", err)
	}
	path := tmp.Name()
	keep := false
	defer func() {
		if !keep {
			_ = tmp.Close()
			_ = os.Remove(path)
		}
	}()
	hash := sha256.New()
	written, err := copySnapshot(ctx, io.MultiWriter(tmp, hash), source, ticket.PayloadBytes)
	if err != nil {
		return nil, err
	}
	if written != ticket.PayloadBytes {
		return nil, fmt.Errorf("cluster: RPC snapshot size mismatch")
	}
	if err := verifyIntegrity(path); err != nil {
		return nil, err
	}
	stamp, err := readSnapshotStamp(path)
	if err != nil {
		return nil, err
	}
	f.mutationMu.Lock()
	err = validateSnapshotInstall(f, stamp, ticket, true)
	f.mutationMu.Unlock()
	if err != nil {
		return nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	w := &f.snapshots
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(w.bindings) != 0 {
		return nil, fmt.Errorf("cluster: snapshot RPC binding already active")
	}
	w.requireTickets = true
	w.bindings = map[[sha256.Size]byte]snapshotBinding{digest: {ticket: ticket}}
	keep = true
	release = false
	return &snapshotRPCStage{file: tmp, path: path, owner: f, digest: digest, ctx: ctx, finish: finish}, nil
}

type snapshotRPCStage struct {
	file   *os.File
	ctx    context.Context
	finish func()
	path   string
	owner  *fsm
	digest [sha256.Size]byte
	once   sync.Once
	err    error
}

func (s *snapshotRPCStage) Read(p []byte) (int, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	return s.file.Read(p)
}

func (s *snapshotRPCStage) Close() error {
	s.once.Do(func() {
		s.err = s.file.Close()
		s.err = errors.Join(s.err, os.Remove(s.path))
		w := &s.owner.snapshots
		w.mu.Lock()
		delete(w.bindings, s.digest)
		w.mu.Unlock()
		s.finish()
	})
	return s.err
}
