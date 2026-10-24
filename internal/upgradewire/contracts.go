// Package upgradewire owns the S8 transaction envelope, distinct from disk and
// adjunct encoding. P13 supplies strict per-operation decoding and explicit
// proto conversions; these declarations alone confer no wire capability.
// origin: hot-update S8 plan §2.2 / §7 / P00
package upgradewire

import (
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

const Version = "upgrade-tx-v1"

// Op remains a string in Request, as frozen for additive wire compatibility.
// The existing enum gate must register this exact closed operation vocabulary.
const (
	OpPrepare     = "upgrade.prepare"
	OpStart       = "upgrade.start"
	OpQuery       = "upgrade.query"
	OpRollback    = "upgrade.rollback"
	OpCancel      = "upgrade.cancel"
	OpArchive     = "upgrade.archive"
	OpCloseWindow = "upgrade.close_window"
	// OpDescribe is additive to the frozen seven: the target reports the
	// identity, running source and original scope a Prepare must name. It is
	// a read with no effect and carries only a selector and a fresh nonce.
	// origin: S8 FIX-DESCRIBE (P19 request R1)
	OpDescribe = "upgrade.describe"
)

// Each operation admits only its own fields. Unknown or duplicate fields and
// oversized collections must be rejected before any endpoint is invoked. Value
// fields and omitempty tags follow the frozen table; the strict codec owns
// omission/presence checks rather than relying on encoding/json's struct zeros.
type Request struct {
	Op         string                   `json:"op"`
	Key        upgradetx.Key            `json:"key"`
	TxID       string                   `json:"tx_id,omitempty"`
	QueryNonce string                   `json:"query_nonce,omitempty"`
	Grant      upgradetx.Grant          `json:"grant,omitempty"`
	Expected   upgradetx.Fence          `json:"expected,omitempty"`
	Prepare    upgradetx.PrepareRequest `json:"prepare,omitempty"`
	Reverse    upgradetx.ReverseRequest `json:"reverse,omitempty"`
	Artifact   upgradeartifact.Receipt  `json:"artifact,omitempty"`
}

type Response struct {
	View  upgradetx.View `json:"view,omitempty"`
	Code  string         `json:"code,omitempty"`
	Error string         `json:"error,omitempty"`
	// Description answers OpDescribe only. A pointer keeps every other
	// response byte-identical to the frozen shape (additive, omitempty).
	// origin: S8 FIX-DESCRIBE (P19 request R1)
	Description *upgradetx.Description `json:"description,omitempty"`
}
