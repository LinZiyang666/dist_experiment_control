package clusterroll

import (
	"crypto/rand"
	"encoding/hex"
	"errors"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func randomHex() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(nonce[:]), nil
}
func rejected(detail string) error {
	return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: detail}
}
func unsettled(detail string) error {
	return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: detail, Accepted: true}
}
func transientQuery(err error) bool {
	var typed *upgradetx.Error
	return errors.As(err, &typed) && typed.Code == upgradetx.CodeDeferred && !typed.Accepted
}

// A lost read confers no new execution authority: retry only the same child
// ID with a fresh challenge, bounded by the original parent's context.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1
func lostAgentQuery(err error) bool {
	var tx *upgradetx.Error
	return errors.As(err, &tx) && tx.Code == upgradetx.CodeUnsettled && tx.Detail == "transaction query reply was lost"
}
