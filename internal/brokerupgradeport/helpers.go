// helpers.go holds the adapter's own small contract helpers. They mirror the
// engine's (internal/brokerupgrade/engine.go) exactly; the adapter does not
// import the engine, which binds it.
// origin: hot-update S8 plan §2.2; S8 BROKER-EXPORTER R-11
package brokerupgradeport

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func refusal(code, detail string) error {
	return &upgradetx.Error{Code: code, Detail: detail}
}

func accepted(err error) error {
	var txErr *upgradetx.Error
	if errors.As(err, &txErr) {
		copyErr := *txErr
		copyErr.Accepted = true
		return &copyErr
	}
	return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "accepted broker upgrade needs current-state recovery", Accepted: true}
}

func activationUnstarted(err error) bool {
	var proof interface{ ActivationUnstarted() bool }
	return errors.As(err, &proof) && proof.ActivationUnstarted()
}

func active(r upgradetx.Record) upgradetx.Attempt {
	if r.Reverse.Handle.TxID != "" {
		return r.Reverse
	}
	return r.Forward
}

func nonce() string {
	var b [16]byte
	// crypto/rand.Read either fills the buffer or terminates the process.
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// digest commits to neutral, non-secret contract values only.
func digest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
