package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"

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

// ValidFingerprint reports whether s is a fingerprint this package could have produced:
// the `SHA256:` tag followed by a raw-standard-base64 SHA-256 digest.
//
// origin: prerelease audit increment 2 internal review, reported by four lanes
// (admission-enforcement/L9-F3, admission-product/L8-F5, adminsock-cli/L10-F4,
// test-blast-radius/EXPLOIT-F1).
//
// WHY A TYPO MUST BE AN ERROR AND NOT A ROW. `tether admin session-allow` writes its
// argument into the session-create allow-list verbatim, and the list is only ever
// consulted by exact match against a fingerprint this function's siblings computed. So a
// mistyped, truncated, or pasted-from-the-wrong-column value is not "an entry that does
// not match yet" — it is an entry that can never match, sitting in the table looking like
// a granted permission. The operator is told "admitted", the user keeps being refused,
// and `--list` shows the fingerprint they were both looking at.
//
// The truncated-paste case is the one that actually happens: `tether admin sessions`
// renders an abbreviated OWNER column, and copying from there produces a prefix that is
// syntactically plausible and permanently useless.
// CANONICAL, NOT MERELY DECODABLE. origin: prerelease audit external review m-1.
// base64's decoder ignores the unused low bits of the final character, so a 32-byte
// digest has several spellings that all decode to the same bytes — flipping the last
// character of a real fingerprint from `A` to `B` is the easy one. Accepting those
// reintroduces precisely the failure this function exists to prevent: the value is
// stored verbatim, the allow-list is only ever consulted by exact string match against
// what fingerprintFromRaw produced, so a non-canonical spelling is admitted, listed,
// and can never match. Strict() rejects the padding bits; re-encoding and comparing
// rejects everything else a future encoding change might let through.
func ValidFingerprint(s string) bool {
	const tag = "SHA256:"
	if !strings.HasPrefix(s, tag) {
		return false
	}
	body := strings.TrimPrefix(s, tag)
	raw, err := base64.RawStdEncoding.Strict().DecodeString(body)
	if err != nil || len(raw) != sha256.Size {
		return false
	}
	return base64.RawStdEncoding.EncodeToString(raw) == body
}
