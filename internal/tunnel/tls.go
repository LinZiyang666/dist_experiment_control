package tunnel

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"time"
)

// generateSelfSignedCert produces a fresh ephemeral P-256 ECDSA cert
// suitable for the tunnel control TLS layer. v1 fallback per
// architecture F.5: "frps 证书来源 ... 否则 frps 回落到自签（仅 v1
// 够用，业务通信安全由业务自己负责）".
//
// Lifetime: 10 years — broker process lifetime is much shorter than
// that, but a too-short cert risks rolling out of validity mid-session.
// CN=tether-broker, no SANs (agents don't verify the cert anyway in
// v1, see Client.tlsConfig).
func generateSelfSignedCert() (tls.Certificate, error) {
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

// serverTLSConfig returns the TLS config the broker tunnel server
// uses on the control listener. With cert == nil we generate a fresh
// ephemeral self-signed cert. Operators who want to pin a real cert
// can pass a pre-loaded tls.Certificate (e.g. shared with the 443
// Caddy via broker config).
func serverTLSConfig(cert *tls.Certificate) (*tls.Config, error) {
	if cert == nil {
		c, err := generateSelfSignedCert()
		if err != nil {
			return nil, err
		}
		cert = &c
	}
	return &tls.Config{
		Certificates: []tls.Certificate{*cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// clientTLSConfig returns the TLS config the agent tunnel client uses
// to dial the broker. v1 sets InsecureSkipVerify because the broker's
// fallback cert is self-signed and the agent has no PKI to compare
// against — confidentiality (passive eavesdropper can't read REGISTER
// or token bytes) is the threat model F.5 actually targets here, and
// that's still satisfied. Active MITM is acknowledged-not-blocked in
// v1; a future revision can add cert pinning derived from the
// broker's nkey identity.
func clientTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, //nolint:gosec // documented v1 fallback per architecture F.5
	}
}
