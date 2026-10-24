package proxyimage

import (
	"encoding/hex"

	"github.com/LinZiyang666/tether/internal/proto"
)

// ProxyState is the control owner's part of the SS private descriptor. It is
// installed while admission is closed, and restored without scheduling work.
// The auxiliary proxy cut also owns routing/readiness metadata; its shared
// authority/timer values must match this state before a callback can be wired.
// origin: hot-update S5 internal review PRIVATE-STATE-2
type ProxyState struct {
	DispatchSequence, LastAppliedSequence int64
	Generation, Epoch                     int64
	KeysetDigest                          [32]byte
	Enabled                               bool
	AllocationSID, AllocationNID          string
	AllocationPort                        int
	AllocationTokenHash                   string
	AllocationEpoch                       int64
	DeadlineBootNS, PlannedDeadlineBootNS int64
	Fired, CleanupPending                 bool
}

func (ProxyState) String() string     { return "ssproxy.ProxyState(redacted)" }
func (p ProxyState) GoString() string { return p.String() }

// ValidProxyState checks the authority and timer metadata before any owner is installed.
func ValidProxyState(p ProxyState) bool {
	if p.DispatchSequence < 0 || p.LastAppliedSequence < 0 || p.LastAppliedSequence > p.DispatchSequence ||
		p.Generation < 0 || p.Epoch < 0 || p.AllocationEpoch < 0 || p.DeadlineBootNS < 0 || p.PlannedDeadlineBootNS < 0 ||
		p.AllocationPort < 0 || p.AllocationPort > 65535 || p.CleanupPending && !p.Fired || p.Fired && p.DeadlineBootNS == 0 {
		return false
	}
	if p.AllocationPort == 0 {
		return p.AllocationSID == "" && p.AllocationNID == "" && p.AllocationTokenHash == "" && p.AllocationEpoch == 0
	}
	hash, err := hex.DecodeString(p.AllocationTokenHash)
	return proto.ValidateSID(p.AllocationSID) == nil && proto.ValidateNID(p.AllocationNID) == nil &&
		err == nil && len(hash) == 32 && hex.EncodeToString(hash) == p.AllocationTokenHash
}

// ProxyCut is the metadata retained by the agent even after its last SS
// listener is gone. It uses the same descriptors and codec as the SS owner;
// there is no opaque private binary envelope hiding a second field schema.
// origin: hot-update S5 internal review PRIVATE-STATE-2
type ProxyCut struct {
	State                                     ProxyState
	LocalPort                                 int
	HomeEpoch                                 int64
	Token, HomeAddr                           string
	DigestApplied, TunnelUp, NeedsReestablish bool
}

func (ProxyCut) String() string     { return "ssproxy.ProxyCut(redacted)" }
func (p ProxyCut) GoString() string { return p.String() }
