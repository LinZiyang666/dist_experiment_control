package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"time"

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
}

func (s *tlsStreamLayer) Accept() (net.Conn, error) { return s.ln.Accept() }
func (s *tlsStreamLayer) Close() error              { return s.ln.Close() }
func (s *tlsStreamLayer) Addr() net.Addr            { return s.ln.Addr() }

func (s *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(d, "tcp", string(address), s.clientTLS)
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

// clusterTLSConfigs builds the (server, client) mutual-TLS configs anchored on
// the cluster CA. Both reject a leaf that does not chain to the CA: the server via
// the standard RequireAndVerifyClientCert path, the client via VerifyPeerCertificate
// (hostname verification is skipped because raft peer addresses carry no SANs).
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
		InsecureSkipVerify:    true, // skip HOSTNAME only (raft addrs have no SANs)
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

	ln, err := tls.Listen("tcp", cfg.BindAddr, serverTLS)
	if err != nil {
		return nil, fmt.Errorf("cluster: mTLS listen %q: %w", cfg.BindAddr, err)
	}
	stream := &tlsStreamLayer{ln: ln, clientTLS: clientTLS}

	maxPool := cfg.MaxPool
	if maxPool == 0 {
		maxPool = 3
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	return raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  stream,
		MaxPool: maxPool,
		Timeout: timeout,
		Logger:  hclog.NewNullLogger(), // discard raft-net chatter (D1 convention)
	}), nil
}
