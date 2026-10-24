// Package xferid holds the identity predicates shared by the continuation
// leaves. It depends only on the standard library.
// origin: hot-update S7 plan §6.2 — S7 production activation
package xferid

import (
	"crypto/sha256"
	"encoding/base64"
	"strings"
)

// ActorFP accepts the canonical actor fingerprint, "SHA256:" plus the unpadded
// standard base64 of a SHA-256 (auth.FingerprintFromActor). That alphabet
// contains ':' '/' and '+', so the subject-token predicates the leaves use for
// their own nonces must not be applied to it: they refused every real actor,
// and only the synthetic fixtures ("actor") ever passed. The fingerprint is
// compared to the authenticated principal and never used as a subject token.
func ActorFP(s string) bool {
	body, ok := strings.CutPrefix(s, "SHA256:")
	if !ok {
		return false
	}
	raw, err := base64.RawStdEncoding.Strict().DecodeString(body)
	return err == nil && len(raw) == sha256.Size && base64.RawStdEncoding.EncodeToString(raw) == body
}
