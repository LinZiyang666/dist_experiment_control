// origin: hot-update S6 plan §2.1 / §12.1 P00
package proto

import (
	"github.com/LinZiyang666/tether/internal/brokermaint"
)

type BrokerImageStatus struct {
	RunningSHA256 string `json:"running_sha256,omitempty"`
	Generation    uint64 `json:"generation,omitempty"`
	Phase         string `json:"phase,omitempty"`
	CutID         string `json:"cut_id,omitempty"`
	Mode          string `json:"mode,omitempty"`
}

type BrokerMaintenanceReq struct {
	Record            brokermaint.Record `json:"record"`
	Proof             brokermaint.Proof  `json:"proof"`
	NID               string             `json:"nid"`
	ConnectedBrokerID string             `json:"connected_broker_id"`
	IngressServerID   string             `json:"ingress_server_id"`
	RouteDigest       string             `json:"route_digest"`
	Sig               string             `json:"sig"`
}

type BrokerMaintenanceAck struct {
	CutID             string `json:"cut_id"`
	Nonce             string `json:"nonce"`
	NID               string `json:"nid"`
	ConnectedBrokerID string `json:"connected_broker_id"`
	IngressServerID   string `json:"ingress_server_id"`
	InstanceID        string `json:"instance_id"`
	RouteDigest       string `json:"route_digest"`
	ProofDigest       string `json:"proof_digest"`
	Accepted          bool   `json:"accepted"`
	RemainingNS       int64  `json:"remaining_ns"`
	Sig               string `json:"sig"`
}

// Status fields stay strings for additive wire compatibility. These constants
// pin the closed phase/mode vocabulary shared by local and remote reporters.
// origin: hot-update S6 plan §9.1
const (
	BrokerImagePhaseServing      = "serving"
	BrokerImagePhasePreparing    = "preparing"
	BrokerImagePhaseFrozen       = "frozen"
	BrokerImagePhaseImporting    = "importing"
	BrokerImagePhaseAuthUsable   = "auth_usable"
	BrokerImagePhaseControlReady = "control_ready"
	BrokerImagePhaseRollingBack  = "rolling_back"
	BrokerImagePhaseFailed       = "failed"
	BrokerImageModeSingle        = "single"
	BrokerImageModeSoleVoter     = "sole-voter"
	BrokerImageModeQuorum        = "quorum"
)
