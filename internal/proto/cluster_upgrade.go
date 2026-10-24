package proto

import (
	"encoding/json"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/upgradeprotocol"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// cluster_upgrade.go (G5 #13 W2b) — the account-signed remote-trigger the `cluster upgrade` orchestrator
// sends to a specific broker over SubjCtrlClusterUpgrade so the roll runs external to all brokers and
// re-resolves the leader after each restart. The security anchor is the account signature (the operator's
// root authority, same trust anchor as the signed roster/seeds): a broker acts ONLY if TargetNode is its
// own node_id AND the signature verifies against its pinned account_pub. ProtoVersion stays 2 (additive).

// ClusterUpgradeReq is one signed remote trigger. Op is "reload" (restart THIS broker into the staged
// on-disk binary) or "transfer-leader" (hand raft leadership to TransferTo).
type ClusterUpgradeReq struct {
	// The transaction envelope and authenticated actor are signed together.
	// Raw JSON is a transport boundary, decoded by upgradewire after signature
	// verification; proto has no dependency on that higher-level adapter.
	// origin: hot-update S8 plan §2.2 / §7.1 / P13
	ActorFP      string          `json:"actor_fp,omitempty"`
	Upgrade      json.RawMessage `json:"upgrade,omitempty"`
	Op           string          `json:"op"`                      // "reload" | "transfer-leader" | "reexec-agent" | "acquire-lock" | "release-lock"
	TargetNode   string          `json:"target_node"`             // the broker (node_id) that must handle this
	TransferTo   string          `json:"transfer_to,omitempty"`   // transfer-leader destination voter
	ToVersion    string          `json:"to_version,omitempty"`    // reload idempotency anchor (skip if already running)
	ExpectSHA256 string          `json:"expect_sha256,omitempty"` // reload/reexec-agent staged-binary digest guard
	SID          string          `json:"sid,omitempty"`           // reexec-agent: the session whose co-located agent to re-exec
	IssuedAt     string          `json:"issued_at"`               // RFC3339; the verifier bounds replay against its clock
	// The extension is signed as one versioned block. Zero extension fields
	// preserve the previous release's bytes, including lock operations; holder
	// admission against a held S6 lock belongs to the live lock owner.
	// origin: hot-update S6 plan §9.1 / T46
	Holder      string              `json:"holder,omitempty"`
	Scope       string              `json:"scope,omitempty"`
	CutID       string              `json:"cut_id,omitempty"`
	Maintenance *brokermaint.Record `json:"maintenance,omitempty"`
	Sig         string              `json:"sig"` // hex(ed25519) over CanonicalUpgradeReqBytes, account-seed signed
}

// ClusterUpgradeResp is the broker's reply to a remote trigger.
type ClusterUpgradeResp struct {
	// Full View is returned only on the targeted, authenticated reply route.
	// origin: hot-update S8 plan §2.2 / §7.1 / P13
	Upgrade          json.RawMessage `json:"upgrade,omitempty"`
	OK               bool            `json:"ok,omitempty"`
	Code             string          `json:"code,omitempty"` // reuses the adminsock Code* vocabulary (is_leader/sha_mismatch/…)
	Error            string          `json:"error,omitempty"`
	Reloading        bool            `json:"reloading,omitempty"`
	AlreadyAtVersion bool            `json:"already_at_version,omitempty"`
	NewLeader        string          `json:"new_leader,omitempty"` // transfer-leader: best-effort new leader id
	// Armed replies report the sealed next generation; completion is proven
	// only by a later status from the image that is actually running.
	// origin: hot-update S6 plan §9.1 / T46
	BrokerImage    *BrokerImageStatus `json:"broker_image,omitempty"`
	NextGeneration uint64             `json:"next_generation,omitempty"`
	// The issuer returns the exact committed record and conservative signed
	// target receipt. Local clocks consume transit time before reload admission.
	// origin: hot-update S6 internal review rv-cluster-3
	Maintenance    *brokermaint.Record   `json:"maintenance,omitempty"`
	MaintenanceAck *BrokerMaintenanceAck `json:"maintenance_ack,omitempty"`
}

// CanonicalUpgradeReqBytes is the EXACT byte sequence signed and verified (it EXCLUDES Sig). Deterministic
// field order with newline separators so an off-cluster signer and every broker verifier agree
// byte-for-byte. Any field change (a swapped TargetNode, a bumped ToVersion) invalidates the signature.
// Invalid inputs return nil. Signers and verifiers must call ValidateUpgradeReq
// before signing/verifying: nil is a failure sentinel, never a signable request.
// origin: hot-update S6 plan §9.1 / T46
func CanonicalUpgradeReqBytes(r *ClusterUpgradeReq) []byte {
	return upgradeprotocol.Canonical(upgradeTrigger(r))
}

// ValidateUpgradeReq rejects ambiguous signed framing and unknown scope. It
// does not grant maintenance or lock authority: live holder/proof/CAS checks
// remain mandatory even after a valid account signature.
// origin: hot-update S6 plan §9.1 / T46
func ValidateUpgradeReq(r *ClusterUpgradeReq) error {
	return upgradeprotocol.Validate(upgradeTrigger(r))
}

// Copy only signed fields across the neutral boundary. The public DTO remains
// owned by proto, and the lower encoder cannot import a transport adapter.
// origin: hot-update S8 plan §7.1 / §12.1 / P13
func upgradeTrigger(r *ClusterUpgradeReq) *upgradeprotocol.Trigger {
	if r == nil {
		return nil
	}
	return &upgradeprotocol.Trigger{Op: r.Op, TargetNode: r.TargetNode, TransferTo: r.TransferTo, ToVersion: r.ToVersion, ExpectSHA256: r.ExpectSHA256, SID: r.SID, IssuedAt: r.IssuedAt, Holder: r.Holder, Scope: r.Scope, CutID: r.CutID, ActorFP: r.ActorFP, Upgrade: r.Upgrade, Maintenance: r.Maintenance}
}

// The neutral transaction package owns these spellings; proto remains below
// every wire adapter and does not import upgradewire or an execution engine.
// origin: hot-update S8 plan §7.1 / P13
const (
	CodeUpgradeNotWired            = upgradetx.CodeNotWired
	CodeUnsupportedComponent       = upgradetx.CodeUnsupportedComponent
	CodeUpgradeStateCorrupt        = upgradetx.CodeStateCorrupt
	CodeUpgradeGrantUnknown        = upgradetx.CodeGrantUnknown
	CodeUpgradeGrantExpired        = upgradetx.CodeGrantExpired
	CodeUpgradeRejected            = upgradetx.CodeRejected
	CodeUpgradeUnsettled           = upgradetx.CodeUnsettled
	CodeUpgradeWindowClosed        = upgradetx.CodeWindowClosed
	CodeSessionIncarnationMismatch = upgradetx.CodeSessionIncarnationMismatch
)

type NodeLease struct {
	// A contested lease cannot discard the inherited authority expectation.
	// origin: hot-update S8 plan §2.2 / §6.2 / P13
	Incarnation string `json:"incarnation,omitempty"`
	// AssignedNID is the lease name this instance must adopt.
	AssignedNID string `json:"assigned_nid"`
	// Basename echoes the name the agent registered with, so the agent can log
	// the transition without re-deriving it.
	Basename string `json:"basename,omitempty"`
}

// UpgradeTxForwardedReq carries the actor stamped by the answering broker.
// The forwarded subject drops the original actor segment; copying a client's
// actor field there would authorize private queries as another session member.
// Only the broker may publish this route (the existing forwarded-command ACL).
// origin: hot-update S8 plan §2.2 / §7.1 / P13
type UpgradeTxForwardedReq struct {
	ActorFP string          `json:"actor_fp"`
	Request json.RawMessage `json:"request"`
}

// These tokens name implemented S8 codec/authority seams; runtime integration
// must establish the actual collectors before advertising hot cut capability.
// origin: hot-update S8 plan §6.1 / P13
const CapUpgradeTXV1 = "upgrade-tx-v1"
const CapAuthorityIncarnationV1 = "authority-incarnation-v1"
const CapUpgradeCutV1 = "upgrade-cut-v1"
