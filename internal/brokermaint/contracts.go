// Package brokermaint defines secret-free maintenance records and cross-clock proofs.
// origin: hot-update S6 plan §2.1 / §12.1 P00
package brokermaint

const Capability = "broker-maint-v1"

// Phase zero is invalid; ended records still retain the historical baseline.
type Phase uint8

const (
	MaintPrepared Phase = iota + 1
	MaintActive
	MaintEnded
)

type Record struct {
	CutID              string `json:"cut_id"`
	Holder             string `json:"holder"`
	Home               string `json:"home"`
	InstanceID         string `json:"instance_id"`
	IssuerBootID       string `json:"issuer_boot_id"`
	Mode               string `json:"mode"`
	IngressServerID    string `json:"ingress_server_id"`
	Generation         uint64 `json:"generation"`
	Phase              Phase  `json:"phase"`
	IssuedBootNS       int64  `json:"issued_boot_ns"`
	ExpiresBootNS      int64  `json:"expires_boot_ns"`
	Term               uint64 `json:"term"`
	ConfigIndex        uint64 `json:"config_index"`
	VoterDigest        string `json:"voter_digest"`
	BaselineDigest     string `json:"baseline_digest"`
	RequiredCapability string `json:"required_capability"`
}

type Proof struct {
	CutID          string `json:"cut_id"`
	Nonce          string `json:"nonce"`
	IssuerBootID   string `json:"issuer_boot_id"`
	ResponseBootNS int64  `json:"response_boot_ns"`
	ExpiresBootNS  int64  `json:"expires_boot_ns"`
	RecordDigest   string `json:"record_digest"`
	AppliedIndex   uint64 `json:"applied_index"`
	Sig            string `json:"sig"`
}

type LocalLease struct {
	Record             Record `json:"record"`
	ExpiresLocalBootNS int64  `json:"expires_local_boot_ns"`
	Proof              Proof  `json:"proof"`
}
