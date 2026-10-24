package ssproxy

import (
	"errors"

	"github.com/LinZiyang666/tether/internal/ssstate"
)

// Unrepresentable semantics and capacity failure are different deferrals.
// Unknown authority never grants permission to decrypt or write target bytes.
// origin: hot-update S5 plan §2.1 / §6.1 / §6.5
var (
	ErrNotImplemented       = errors.New("ssproxy: not implemented (S5 P8)")
	ErrCodecNotImplemented  = errors.New("ssproxy: not implemented (S5 P9)")
	ErrImageNotImplemented  = errors.New("ssproxy: not implemented (S5 P10)")
	ErrStateUnrepresentable = ssstate.ErrStateUnrepresentable
	ErrCapacity             = errors.New("ssproxy: capacity exceeded")
	ErrAuthorityPending     = errors.New("ssproxy: authority pending")
)

const (
	Version            = 1
	MaxActors          = 128
	MaxMaterialHistory = 64
	MaxKeys            = 256
)

type SSPhase uint8

const (
	SSPhaseSalt       SSPhase = 1
	SSPhaseLength     SSPhase = 2
	SSPhasePayload    SSPhase = 3
	SSPhaseTarget     SSPhase = 4
	SSPhaseConnecting SSPhase = 5
	SSPhaseRelay      SSPhase = 6
	SSPhaseDraining   SSPhase = 7
	SSPhaseRevoked    SSPhase = 8
	SSPhaseTerminal   SSPhase = 9
)

type AuthorityPhase uint8

const (
	AuthorityPhasePending    AuthorityPhase = 1
	AuthorityPhaseAuthorized AuthorityPhase = 2
	AuthorityPhaseRevoked    AuthorityPhase = 3
)

// Image is public metadata. Keys, salts, nonces, targets and pending bytes
// belong exclusively in the sealed state referred to by StateRef.
type Image struct {
	Version         int    `json:"version"`
	StateRef        string `json:"state_ref"`
	ListenerRef     string `json:"listener_ref"`
	ConnectionCount int    `json:"connection_count"`
	SecretBytes     int64  `json:"secret_bytes"`
}
type Grant struct {
	Enabled         bool
	Generation      int64
	Epoch           int64
	AppliedRevision uint64
	Keys            []Key
}
type Frozen struct{ state *frozenState }
