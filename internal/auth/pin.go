// Package auth implements the three authentication primitives used by tetherd
// and the agent/CLI clients:
//
//   - PIN: argon2id hashing for session join (requirements §6.3, §9).
//   - nkey: ed25519 key generation/derivation/sign/verify; doubles as both
//     the application-layer identity (fingerprint) and the NATS user nkey.
//   - JWT: account-signed per-connection user JWT, returned from auth_callout
//     (architecture E.2 / E.5).
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// argon2id parameters per requirements §6.3 / OWASP recommendation. Fixed in
// v1; rotation needs a new audit version + migration plan.
const (
	pinMemoryKiB   uint32 = 64 * 1024 // 64 MiB
	pinIterations  uint32 = 3
	pinParallelism uint8  = 2
	pinSaltLen            = 16
	pinKeyLen      uint32 = 32
)

// HashPIN derives an argon2id hash and returns the canonical PHC string
//
//	$argon2id$v=19$m=65536,t=3,p=2$<base64salt>$<base64hash>
//
// suitable for storage in `sessions.pin_hash`.
func HashPIN(pin string) (string, error) {
	if err := ValidPIN(pin); err != nil {
		return "", err
	}
	salt := make([]byte, pinSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("pin: read salt: %w", err)
	}
	hash := argon2.IDKey([]byte(pin), salt,
		pinIterations, pinMemoryKiB, pinParallelism, pinKeyLen)
	return encodePHC(salt, hash), nil
}

// VerifyPIN compares pin against a PHC-encoded argon2id hash in constant time.
// Returns nil only on match; non-nil error on any divergence (parameter
// mismatch, malformed PHC, wrong PIN).
func VerifyPIN(pin, phc string) error {
	salt, hash, err := decodePHC(phc)
	if err != nil {
		return err
	}
	check := argon2.IDKey([]byte(pin), salt,
		pinIterations, pinMemoryKiB, pinParallelism, pinKeyLen)
	if subtle.ConstantTimeCompare(check, hash) != 1 {
		return errors.New("pin: mismatch")
	}
	return nil
}

// ValidPIN enforces the requirements §6.3 character set: ASCII printable
// (0x20–0x7E). No length / complexity rules in v1.
func ValidPIN(pin string) error {
	if pin == "" {
		return errors.New("pin: empty")
	}
	for _, r := range pin {
		if r < 0x20 || r > 0x7E {
			return errors.New("pin: must be ASCII printable (0x20-0x7E)")
		}
	}
	return nil
}

func encodePHC(salt, hash []byte) string {
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		pinMemoryKiB, pinIterations, pinParallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	)
}

func decodePHC(phc string) (salt, hash []byte, err error) {
	parts := strings.Split(phc, "$")
	// Expected: ["", "argon2id", "v=19", "m=...,t=...,p=...", saltb64, hashb64]
	if len(parts) != 6 || parts[1] != "argon2id" {
		return nil, nil, errors.New("phc: malformed (expected $argon2id$v=...$m=...,t=...,p=...$salt$hash)")
	}
	var version int
	if _, err = fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return nil, nil, fmt.Errorf("phc: version: %w", err)
	}
	if version != argon2.Version {
		return nil, nil, fmt.Errorf("phc: argon2 version mismatch (have %d, expected %d)", version, argon2.Version)
	}
	var m, t uint32
	var p uint8
	if _, err = fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &m, &t, &p); err != nil {
		return nil, nil, fmt.Errorf("phc: params: %w", err)
	}
	if m != pinMemoryKiB || t != pinIterations || p != pinParallelism {
		return nil, nil, fmt.Errorf(
			"phc: parameter mismatch (have m=%d t=%d p=%d, expected m=%d t=%d p=%d)",
			m, t, p, pinMemoryKiB, pinIterations, pinParallelism)
	}
	if salt, err = base64.RawStdEncoding.DecodeString(parts[4]); err != nil {
		return nil, nil, fmt.Errorf("phc: salt: %w", err)
	}
	if hash, err = base64.RawStdEncoding.DecodeString(parts[5]); err != nil {
		return nil, nil, fmt.Errorf("phc: hash: %w", err)
	}
	return salt, hash, nil
}
