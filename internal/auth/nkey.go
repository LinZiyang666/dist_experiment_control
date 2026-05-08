package auth

import (
	"errors"
	"fmt"

	"github.com/nats-io/nkeys"
)

// GenerateUserSeed returns a fresh ed25519 user-nkey seed (the "S..." form
// bytes). The seed is the only secret material; the public key is derived.
//
// nkeys.KeyPair.Seed() returns a slice that aliases the keypair's internal
// buffer; Wipe() then overwrites that buffer with random bytes. We copy the
// seed before wiping so the returned slice survives.
func GenerateUserSeed() ([]byte, error) {
	kp, err := nkeys.CreateUser()
	if err != nil {
		return nil, fmt.Errorf("nkey: create user: %w", err)
	}
	seed, err := kp.Seed()
	if err != nil {
		kp.Wipe()
		return nil, fmt.Errorf("nkey: extract seed: %w", err)
	}
	out := append([]byte(nil), seed...)
	kp.Wipe()
	return out, nil
}

// PublicKeyFromSeed re-derives the nkey public key (e.g. "U...") from the
// provided seed. Wraps nkeys.FromSeed + KeyPair.PublicKey so callers don't
// need to import nkeys.
func PublicKeyFromSeed(seed []byte) (string, error) {
	kp, err := nkeys.FromSeed(seed)
	if err != nil {
		return "", fmt.Errorf("nkey: parse seed: %w", err)
	}
	defer kp.Wipe()
	return kp.PublicKey()
}

// SignWithSeed signs msg using the seed's private key. Output is 64 bytes.
func SignWithSeed(seed, msg []byte) ([]byte, error) {
	kp, err := nkeys.FromSeed(seed)
	if err != nil {
		return nil, fmt.Errorf("nkey: parse seed: %w", err)
	}
	defer kp.Wipe()
	return kp.Sign(msg)
}

// VerifySignature checks sig against pub (an nkey public key string).
// Returns nil only when the signature verifies.
func VerifySignature(pub string, msg, sig []byte) error {
	kp, err := nkeys.FromPublicKey(pub)
	if err != nil {
		return fmt.Errorf("nkey: parse public key: %w", err)
	}
	if err := kp.Verify(msg, sig); err != nil {
		return errors.New("nkey: signature mismatch")
	}
	return nil
}
