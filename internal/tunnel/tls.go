package tunnel

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel/transport"
)

// dialFlowTLS finishes TLS before its caller may send REGISTER. The one allowed
// retry clones the same pin/PKI settings and shares the original operation ctx,
// so it neither extends the deadline nor changes identity. A retained v1 flow
// requires v1 on every attachment, including an old peer that returns no ALPN.
// origin: hot-update S5 plan §4.1 / T15
func dialFlowTLS(ctx context.Context, network, addr string, dialer *net.Dialer, cfg *tls.Config, retained bool) (*tls.Conn, error) {
	// TLS applies NetDialer.Timeout anew for each DialContext. Convert it to
	// one operation deadline before retrying so fallback cannot renew the budget.
	// An earlier caller or NetDialer absolute deadline still wins.
	// origin: hot-update S5 plan §4.1 / T15
	if dialer != nil && dialer.Timeout > 0 {
		operation, cancel := context.WithTimeout(ctx, dialer.Timeout)
		defer cancel()
		ctx = operation
	}
	attempt := cfg.Clone()
	for retry := 0; ; retry++ {
		td := tls.Dialer{NetDialer: dialer, Config: attempt}
		conn, err := td.DialContext(ctx, network, addr)
		if err != nil {
			if retry == 0 && !retained && len(attempt.NextProtos) == 1 &&
				attempt.NextProtos[0] == FlowALPN && isRemoteALPNRefusal(err) {
				attempt = cfg.Clone()
				attempt.NextProtos = nil
				continue
			}
			return nil, err
		}
		// origin: hot-update S5 plan §4.1 / T15
		secure, ok := conn.(*tls.Conn) // tls.Dialer returns the completed TLS connection.
		if !ok {
			_ = conn.Close()
			return nil, errors.New("tunnel: TLS dial returned an invalid connection")
		}
		if _, err := flowProtocol(secure.ConnectionState(), retained); err != nil {
			_ = secure.Close()
			return nil, err
		}
		return secure, nil
	}
}

// The runtime retains its API while the leaf owns this implementation.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func flowServerTLSConfig(base *tls.Config, ready bool) *tls.Config {
	return transport.FlowServerTLSConfig(base, ready)
}
func flowClientTLSConfig(base *tls.Config, ready bool) *tls.Config {
	return transport.FlowClientTLSConfig(base, ready)
}
func flowProtocol(state tls.ConnectionState, retained bool) (bool, error) {
	return transport.FlowProtocol(state, retained)
}
func generateSelfSignedCert() (tls.Certificate, error) { return transport.GenerateSelfSignedCert() }
func clientTLSConfig() *tls.Config                     { return transport.ClientTLSConfig() }

func LoadServerCert(certPEM, keyPEM string) (*tls.Certificate, error) {
	return transport.LoadServerCert(certPEM, keyPEM)
}
func CertFingerprint(cert *x509.Certificate) string { return transport.CertFingerprint(cert) }

func clientTLSConfigPinned(pins proto.CertPins) *tls.Config {
	return transport.ClientTLSConfigPinned(pins.Current, pins.Previous, pins.ValidUntil)
}

func isRemoteALPNRefusal(err error) bool { return transport.IsRemoteALPNRefusal(err) }

func certFingerprintDER(der []byte) string { return transport.CertFingerprintDER(der) }
