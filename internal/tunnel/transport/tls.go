package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net"
	"runtime"
	"time"

	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
)

// flowServerTLSConfig is opt-in: the caller must have installed the complete
// flow owner, bounded authorization and restore support before passing ready.
// Clone both configurations so a legacy ClientHello cannot remove ALPN from
// another concurrent handshake. Certificate rotation and verification callbacks
// remain owned by the original configuration.
// origin: hot-update S5 plan §4.1 / T15
func FlowServerTLSConfig(base *tls.Config, ready bool) *tls.Config {
	cfg := base.Clone()
	available := ready && runtime.GOOS == "linux"
	cfg.NextProtos = nil
	if available {
		cfg.NextProtos = []string{flowstate.FlowALPN}
	}
	cfg.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		selected := base
		if base.GetConfigForClient != nil {
			custom, err := base.GetConfigForClient(hello)
			if err != nil {
				return nil, err
			}
			if custom != nil {
				selected = custom
			}
		}
		perHello := selected.Clone()
		perHello.NextProtos = nil
		for _, protocol := range hello.SupportedProtos {
			if available && protocol == flowstate.FlowALPN {
				perHello.NextProtos = []string{flowstate.FlowALPN}
				break
			}
		}
		return perHello, nil
	}
	return cfg
}

// Advertising the image reader domain is independent of runtime capability:
// Darwin retains ordinary raw expose, even if the binary understands the image.
// origin: hot-update S5 plan §4.1 / §7.4
func FlowClientTLSConfig(base *tls.Config, ready bool) *tls.Config {
	cfg := base.Clone()
	cfg.NextProtos = nil
	if ready && runtime.GOOS == "linux" {
		cfg.NextProtos = []string{flowstate.FlowALPN}
	}
	return cfg
}

// Only TLS's remote ALPN refusal permits a retry. Pin failures, local alerts,
// timeouts and certificate errors must never turn into an unverified dial.
// The inner alert is checked exactly, not by substring in a wrapped message.
// origin: hot-update S5 plan §4.1 / T15
func IsRemoteALPNRefusal(err error) bool {
	var remote *net.OpError
	return errors.As(err, &remote) && remote.Op == "remote error" &&
		remote.Err != nil && remote.Err.Error() == "tls: no application protocol"
}

// Empty ALPN is legacy only before v1 responsibility exists. Once v1 is
// selected, REGISTER/RESUME/frame errors are terminal for this attachment;
// callers must propagate them rather than re-entering dial with legacy config.
// origin: hot-update S5 plan §4.1 / T15
func FlowProtocol(state tls.ConnectionState, retained bool) (bool, error) {
	switch state.NegotiatedProtocol {
	case flowstate.FlowALPN:
		if runtime.GOOS != "linux" {
			return false, fmt.Errorf("tunnel tls: flow protocol unsupported on this platform")
		}
		return true, nil
	case "":
		if retained {
			return false, fmt.Errorf("tunnel tls: retained flow requires v1 carrier")
		}
		return false, nil
	default:
		return false, fmt.Errorf("tunnel tls: unsupported negotiated protocol")
	}
}

// generateSelfSignedCert produces a fresh ephemeral P-256 ECDSA cert
// suitable for the tunnel control TLS layer. v1 fallback per
// architecture F.5: "frps 证书来源 ... 否则 frps 回落到自签（仅 v1
// 够用，业务通信安全由业务自己负责）".
//
// Lifetime: 10 years — broker process lifetime is much shorter than
// that, but a too-short cert risks rolling out of validity mid-session.
// CN=tether-broker, no SANs (agents don't verify the cert anyway in
// v1, see Client.tlsConfig).
func GenerateSelfSignedCert() (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("tunnel tls: gen key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("tunnel tls: serial: %w", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "tether-broker"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("tunnel tls: create cert: %w", err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
	}, nil
}

// clientTLSConfig returns the TLS config the agent tunnel client uses
// to dial the broker. v1 sets InsecureSkipVerify because the broker's
// fallback cert is self-signed and the agent has no PKI to compare
// against — confidentiality (passive eavesdropper can't read REGISTER
// or token bytes) is the threat model F.5 actually targets here, and
// that's still satisfied. Active MITM is acknowledged-not-blocked in
// v1; D6 adds cert pinning (clientTLSConfigPinned) for clustered homes.
func ClientTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		// Documented v1 fallback per architecture F.5/§16.7 (N=1): no cluster CA and no pins, so there
		// is nothing to verify against. Registered in unverifiedTLSFallbacks with the full argument.
		InsecureSkipVerify: true,
	}
}

// LoadServerCert parses a stable tunnel server certificate + key from PEM bytes
// (D6 §15 RF3). Used by the broker tunnel server (via NewServerWithCert) to pin
// a persistent cert whose fingerprint the agent verifies, instead of the
// ephemeral self-signed fallback. In D6 build-and-prove this is harness-only;
// production stays on the ephemeral path (cutover = D9). The leaf's DER is
// validated so CertFingerprint and the agent's pin agree.
func LoadServerCert(certPEM, keyPEM string) (*tls.Certificate, error) {
	cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		return nil, fmt.Errorf("tunnel tls: load server cert: %w", err)
	}
	if len(cert.Certificate) == 0 {
		return nil, fmt.Errorf("tunnel tls: loaded cert has no leaf DER")
	}
	return &cert, nil
}

// CertFingerprint is the SINGLE SOURCE OF TRUTH for a tunnel cert pin (D6
// §7.7/§15/R-20): "sha256:" + hex(SHA-256(leaf DER)). The same function is used
// by the harness/operator that seeds cluster_nodes.cert_fp AND by the agent that
// verifies the home's presented cert — a divergent formula would silently bypass
// pinning. It hashes cert.Raw (the leaf certificate DER), NOT the SPKI.
func CertFingerprint(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.Raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// certFingerprintDER is the raw-DER variant used inside VerifyConnection (where
// only the parsed leaf's .Raw is in hand). Identical output to CertFingerprint.
func CertFingerprintDER(der []byte) string {
	sum := sha256.Sum256(der)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// clientTLSConfigPinned returns the agent dial TLS config for one expose (D6
// §7.7/R-21). With EMPTY pins (pins.Current == "") it is the legacy N=1 fallback
// — InsecureSkipVerify, no callback, byte-equivalent to clientTLSConfig (the
// ONLY remaining InsecureSkipVerify path, §16.7). With NON-EMPTY pins it keeps
// InsecureSkipVerify (the home cert is self-signed, no CA chain) but installs a
// VerifyConnection callback — which runs on EVERY handshake INCLUDING TLS 1.3
// session resumption (VerifyPeerCertificate does NOT) — that fingerprints the
// presented leaf and accepts it iff it matches Current, or Previous within an
// unexpired rotation window. The handshake fails BEFORE the REGISTER token is
// written, so a MITM presenting a non-pinned cert never receives the bearer
// token. Fail-closed: empty peer certs, or a Previous with no/zero ValidUntil,
// are rejected.
// Receive pin values from the policy owner. Importing the public protocol here
// would couple TLS verification to authentication and transaction runtimes.
// origin: S7 internal review GATE-CONVERGE-layering F3
func ClientTLSConfigPinned(current, previous string, validUntil *time.Time) *tls.Config {
	if current == "" {
		return ClientTLSConfig()
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		// Hostname verification off, peer verification ON: the pin check is in VerifyConnection below.
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("tunnel tls: home presented no certificate")
			}
			fp := CertFingerprintDER(cs.PeerCertificates[0].Raw)
			if fp == current {
				return nil
			}
			if previous != "" && validUntil != nil && time.Now().Before(*validUntil) && fp == previous {
				return nil
			}
			return fmt.Errorf("tunnel tls: home cert %s not in pin set", fp)
		},
	}
}
