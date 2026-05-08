package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"github.com/nats-io/nkeys"
)

// FingerprintFromActor converts a NATS user nkey public key (e.g. "U...")
// to the OpenSSH-style fingerprint "SHA256:<base64-no-pad>" that
// requirements §3 specifies as the canonical identity (`actor_pubkey_fp`).
//
// Identity = SHA-256 of the *raw* 32-byte ed25519 public key, base64
// without padding, prefixed with "SHA256:".
func FingerprintFromActor(actor string) (string, error) {
	raw, err := nkeys.Decode(nkeys.PrefixByteUser, []byte(actor))
	if err != nil {
		return "", fmt.Errorf("auth: decode actor pubkey: %w", err)
	}
	return fingerprintFromRaw(raw), nil
}

// FingerprintFromSeed loads the seed, derives the public key, and returns
// the fingerprint. Convenience for callers that hold a seed already.
func FingerprintFromSeed(seed []byte) (string, error) {
	pub, err := PublicKeyFromSeed(seed)
	if err != nil {
		return "", err
	}
	return FingerprintFromActor(pub)
}

func fingerprintFromRaw(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "SHA256:" + base64.RawStdEncoding.EncodeToString(sum[:])
}
