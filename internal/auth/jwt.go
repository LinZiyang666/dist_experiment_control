package auth

import (
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// AccountSigner wraps an account-level nkey seed used to sign per-connection
// user JWTs. tetherd holds exactly one and uses it from inside its
// auth_callout response handler (architecture E.5).
type AccountSigner struct{ seed []byte }

// LoadAccountSigner accepts a raw account nkey seed (the "SA..." form's
// bytes) and validates both that it parses AND that the derived public key is
// an account key (A...). User seeds (SU.../U...) are rejected: the
// auth_callout flow only makes sense when tetherd signs JWTs from an account
// identity, so accepting a user seed here would silently produce JWTs with a
// wrong-kind issuer.
func LoadAccountSigner(seed []byte) (*AccountSigner, error) {
	kp, err := nkeys.FromSeed(seed)
	if err != nil {
		return nil, fmt.Errorf("auth: invalid account seed: %w", err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		kp.Wipe()
		return nil, fmt.Errorf("auth: derive account public key: %w", err)
	}
	if !nkeys.IsValidPublicAccountKey(pub) {
		kp.Wipe()
		return nil, fmt.Errorf("auth: seed is not an account seed (derived public key %q does not start with 'A')", pub)
	}
	kp.Wipe()
	dup := make([]byte, len(seed))
	copy(dup, seed)
	return &AccountSigner{seed: dup}, nil
}

// IssueUserJWT signs a user JWT for the NATS user public key `userPub` with
// the supplied permissions. ttl=0 means "no expiry encoded".
//
// `userPub` must be a NATS user public key (`U…`). Empty / wrong-kind /
// malformed inputs are rejected as ordinary errors; jwt.NewUserClaims("")
// returns nil and would otherwise panic at the next field access (round-2
// review F1).
func (a *AccountSigner) IssueUserJWT(userPub string, perms jwt.Permissions, ttl time.Duration) (string, error) {
	if !nkeys.IsValidPublicUserKey(userPub) {
		return "", fmt.Errorf("jwt: userPub %q is not a valid NATS user public key (expect U…)", userPub)
	}

	kp, err := nkeys.FromSeed(a.seed)
	if err != nil {
		return "", fmt.Errorf("jwt: parse account seed: %w", err)
	}
	defer kp.Wipe()

	uc := jwt.NewUserClaims(userPub)
	uc.Permissions = perms
	if ttl > 0 {
		uc.Expires = time.Now().Add(ttl).Unix()
	}
	token, err := uc.Encode(kp)
	if err != nil {
		return "", fmt.Errorf("jwt: encode: %w", err)
	}
	return token, nil
}

// AccountPublicKey returns the public key of the issuing account; useful for
// asserting "this JWT was signed by us" in tests / introspection tools.
func (a *AccountSigner) AccountPublicKey() (string, error) {
	kp, err := nkeys.FromSeed(a.seed)
	if err != nil {
		return "", err
	}
	defer kp.Wipe()
	return kp.PublicKey()
}

// DecodeUserJWT parses & verifies the signature against the issuer key
// embedded in the claims, returning the user claims. Membership /
// authorization checks happen elsewhere — this only proves the token is
// well-formed and was signed by the claimed issuer.
func DecodeUserJWT(token string) (*jwt.UserClaims, error) {
	uc, err := jwt.DecodeUserClaims(token)
	if err != nil {
		return nil, fmt.Errorf("jwt: decode: %w", err)
	}
	return uc, nil
}
