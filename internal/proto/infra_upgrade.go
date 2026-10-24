package proto

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nkeys"
)

// Infra has a separate subject, purpose and signature domain. Adding operands
// to the legacy reload trigger could make an older broker reexec itself after
// silently discarding those operands. Its signing bytes remain owned by S8.
// origin: hot-update S9 plan §7.1 / T46
const InfraRequestVersion = 1
const InfraRequestPurpose = "infra-request-v1"
const InfraRequestSigningDomain = "tether-infra-request-v1\n"
const InfraMovePurpose = "infra-move-v1"
const InfraMaxRequestBytes = 65536

const (
	CodeInfraUnsupported         = "infra_unsupported"
	CodeInfraArtifactInvalid     = "infra_artifact_invalid"
	CodeInfraConfigInvalid       = "infra_config_invalid"
	CodeInfraIncompatible        = "infra_incompatible"
	CodeInfraPermissionDenied    = "infra_permission_denied"
	CodeInfraPolicyStale         = "infra_policy_stale"
	CodeInfraNotEnabled          = "infra_not_enabled"
	CodeInfraBusy                = "infra_busy"
	CodeInfraNotReady            = "infra_not_ready"
	CodeInfraInventoryIncomplete = "infra_inventory_incomplete"
	CodeInfraBudgetExceeded      = "infra_budget_exceeded"
	CodeInfraResultUnknown       = "infra_result_unknown"
	CodeInfraRecoveryRequired    = "infra_recovery_required"
	CodeInfraInternal            = "infra_internal"
)

// InfraCommand contains public selectors only. The local owner resolves these
// into a private Plan; no caller can supply a root path, argv, process identity,
// store lock or startup fence. HostSupport is deliberately outside this surface.
// Every field is required on this new versioned protocol, including zero fields.
type InfraCommand struct {
	Version      uint16 `json:"version"`
	Purpose      string `json:"purpose"`
	Op           string `json:"op"`
	Component    string `json:"component"`
	Action       string `json:"action"`
	TxID         string `json:"tx_id"`
	ToVersion    string `json:"to_version"`
	ExpectSHA256 string `json:"expect_sha256"`
	ConfigSHA256 string `json:"config_sha256"`
	RemainingNS  uint64 `json:"remaining_ns"`
}

// InfraUpgradeReq binds the account's signature to the exact targeted host,
// authenticated management identity, nonce and remaining attempt budget.
type InfraUpgradeReq struct {
	Request    InfraCommand `json:"request"`
	TargetNode string       `json:"target_node"`
	Actor      string       `json:"actor"`
	IssuedAt   string       `json:"issued_at"`
	Nonce      string       `json:"nonce"`
	Sig        string       `json:"sig"`
}

// InfraCapability is only emitted by a fully wired runtime probe. Absence in
// an old health/admin reply means unsupported, never ready by default.
type InfraCapability struct {
	Version uint16 `json:"version"`
	NATS    bool   `json:"nats"`
	Caddy   bool   `json:"caddy"`
}

// InfraReport is the public allowlist. Detail, raw sockets, private paths,
// boot/invocation identities and ACME material cannot enter this projection.
type InfraReport struct {
	TxID            string `json:"tx_id,omitempty"`
	Step            uint32 `json:"step,omitempty"`
	Phase           string `json:"phase,omitempty"`
	Revision        uint64 `json:"revision,omitempty"`
	Code            string `json:"code,omitempty"`
	RunningSHA256   string `json:"running_sha256,omitempty"`
	LiveBusiness    uint32 `json:"live_business,omitempty"`
	OldOwnerPending uint32 `json:"old_owner_pending,omitempty"`
	Proof           string `json:"proof,omitempty"`
}

type InfraUpgradeResp struct {
	OK         bool             `json:"ok,omitempty"`
	Code       string           `json:"code,omitempty"`
	Error      string           `json:"error,omitempty"`
	Capability *InfraCapability `json:"capability,omitempty"`
	Report     *InfraReport     `json:"report,omitempty"`
}

// Move is private broker-to-owner control. The typed proof is validated by the
// frozen neutral route codec; an opaque payload cannot evade its field checks.
type InfraMoveReq struct {
	Version uint16       `json:"version"`
	Purpose string       `json:"purpose"`
	SID     string       `json:"sid"`
	NID     string       `json:"nid"`
	Proof   c.RouteProof `json:"proof"`
}

func infraInvalid() error { return errors.New(CodeInfraConfigInvalid) }

// Canonical comparison provides the same closed-field guarantee as the neutral
// host codec without importing upgradewire (which itself imports proto). It
// rejects duplicate keys, aliases, omitted fields and trailing bytes before any
// signature or effect. Allocation is bounded before the typed JSON decoder.
// origin: hot-update S9 plan §2.2 / §7.1 / T46
func decodeInfraCanonical(body []byte, target any) error {
	if len(body) == 0 || len(body) > InfraMaxRequestBytes || !utf8.Valid(body) {
		return infraInvalid()
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return infraInvalid()
	}
	encoded, err := json.Marshal(target)
	if err != nil || !bytes.Equal(encoded, body) {
		return infraInvalid()
	}
	return nil
}

func infraHex(s string, size int) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == size && hex.EncodeToString(b) == s && s != strings.Repeat("0", size*2)
}

func ValidateInfraCommand(r InfraCommand) error {
	if r.Version != InfraRequestVersion || r.Purpose != InfraRequestPurpose {
		return infraInvalid()
	}
	if r.RemainingNS == 0 || r.RemainingNS > uint64(120*time.Second) {
		return errors.New(CodeInfraBudgetExceeded)
	}
	if r.Component != "" && r.Component != "nats" && r.Component != "caddy" {
		return errors.New(CodeInfraUnsupported)
	}
	switch r.Op {
	case "probe":
		if r.Action != "" || r.TxID != "" || r.ToVersion != "" || r.ExpectSHA256 != "" || r.ConfigSHA256 != "" {
			return infraInvalid()
		}
	case "status", "recover":
		if !infraHex(r.TxID, 16) || r.Action != "" || r.Component != "" || r.ToVersion != "" || r.ExpectSHA256 != "" || r.ConfigSHA256 != "" {
			return infraInvalid()
		}
	case "start":
		if !infraHex(r.TxID, 16) || r.Component == "" {
			return infraInvalid()
		}
		switch r.Action {
		case "upgrade":
			if len(r.ToVersion) == 0 || len(r.ToVersion) > 128 || strings.ContainsAny(r.ToVersion, "\r\n /\\\x00") || !utf8.ValidString(r.ToVersion) || !infraHex(r.ExpectSHA256, 32) || r.ConfigSHA256 != "" {
				return infraInvalid()
			}
		case "rollback":
			if r.ToVersion != "" || r.ExpectSHA256 != "" || r.ConfigSHA256 != "" {
				return infraInvalid()
			}
		case "reload":
			if r.Component != "caddy" || !infraHex(r.ConfigSHA256, 32) || r.ToVersion != "" || r.ExpectSHA256 != "" {
				return infraInvalid()
			}
		default:
			return infraInvalid()
		}
	default:
		return infraInvalid()
	}
	return nil
}

func EncodeInfraUpgradeRequest(r InfraUpgradeReq) ([]byte, error) {
	if err := ValidateInfraCommand(r.Request); err != nil {
		return nil, err
	}
	when, err := time.Parse(time.RFC3339Nano, r.IssuedAt)
	if err != nil || when.UTC().Format(time.RFC3339Nano) != r.IssuedAt || ValidateClusterNodeID(r.TargetNode) != nil || !nkeys.IsValidPublicUserKey(r.Actor) || !infraHex(r.Nonce, 16) || (r.Sig != "" && !infraHex(r.Sig, 64)) {
		return nil, infraInvalid()
	}
	return json.Marshal(r)
}

func DecodeInfraUpgradeRequest(body []byte) (InfraUpgradeReq, error) {
	var r InfraUpgradeReq
	if err := decodeInfraCanonical(body, &r); err != nil {
		return r, err
	}
	_, err := EncodeInfraUpgradeRequest(r)
	return r, err
}

func InfraUpgradeSigningBytes(r InfraUpgradeReq) ([]byte, error) {
	r.Sig = ""
	body, err := EncodeInfraUpgradeRequest(r)
	if err != nil {
		return nil, err
	}
	return append([]byte(InfraRequestSigningDomain), body...), nil
}

// Transport authorization is separate from root policy/domain admission. This
// verifier binds the authenticated identity and its private reply route before
// an adapter may answer, preventing a valid publisher from injecting responses
// into someone else's inbox. No timeout or absent responder authorizes reload.
// origin: hot-update S9 plan §7.1 / T46 / T47
func VerifyInfraUpgradeRequest(r InfraUpgradeReq, accountPub, actor, targetNode, replyPrefix, reply string, now time.Time) error {
	body, err := InfraUpgradeSigningBytes(r)
	when, timeErr := time.Parse(time.RFC3339Nano, r.IssuedAt)
	key, keyErr := nkeys.FromPublicKey(accountPub)
	sig, sigErr := hex.DecodeString(r.Sig)
	if err != nil || timeErr != nil || keyErr != nil || sigErr != nil || !nkeys.IsValidPublicAccountKey(accountPub) || actor != r.Actor || targetNode != r.TargetNode || now.Before(when) || now.Sub(when) > 2*time.Minute || !ValidInfraReply(replyPrefix, reply) {
		return errors.New(CodeInfraPermissionDenied)
	}
	defer key.Wipe()
	if key.Verify(body, sig) != nil {
		return errors.New(CodeInfraPermissionDenied)
	}
	return nil
}

func ValidInfraReply(prefix, reply string) bool {
	if !strings.HasPrefix(prefix, "_TINBOX.") || len(reply) > 512 || !strings.HasPrefix(reply, prefix+".") {
		return false
	}
	for _, token := range strings.Split(strings.TrimPrefix(reply, prefix+"."), ".") {
		if token == "" {
			return false
		}
		for _, ch := range token {
			if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '_' && ch != '-' {
				return false
			}
		}
	}
	return true
}

func EncodeInfraMoveRequest(r InfraMoveReq) ([]byte, error) {
	if r.Version != InfraRequestVersion || r.Purpose != InfraMovePurpose || ValidateSID(r.SID) != nil || ValidateNID(r.NID) != nil || len(r.Proof.Signature) != 64 {
		return nil, infraInvalid()
	}
	if _, err := c.EncodeRouteProof(r.Proof); err != nil {
		return nil, infraInvalid()
	}
	return json.Marshal(r)
}

func DecodeInfraMoveRequest(body []byte, sid, nid string) (InfraMoveReq, error) {
	var r InfraMoveReq
	if err := decodeInfraCanonical(body, &r); err != nil {
		return r, err
	}
	if r.SID != sid || r.NID != nid {
		return r, errors.New(CodeInfraPermissionDenied)
	}
	_, err := EncodeInfraMoveRequest(r)
	return r, err
}

func SubjClusterInfra(nodeID string) string {
	return SubjClusterPrefix + ".infra." + nodeID + ".req"
}

func ParseClusterInfra(subject string) (nodeID string, ok bool) {
	parts := strings.Split(subject, ".")
	if len(parts) != 6 || parts[0]+"."+parts[1]+"."+parts[2] != SubjClusterPrefix || parts[3] != "infra" || parts[5] != "req" || ValidateClusterNodeID(parts[4]) != nil {
		return "", false
	}
	return parts[4], true
}

func SubjInfraMove(sid, nid string) string {
	return SubjectPrefix + ".ctrl.s." + sid + ".node." + nid + ".infra-move.req"
}

func ParseInfraMove(subject string) (sid, nid string, ok bool) {
	parts := strings.Split(subject, ".")
	if len(parts) != 9 || parts[0]+"."+parts[1] != SubjectPrefix || parts[2] != "ctrl" || parts[3] != "s" || parts[5] != "node" || parts[7] != "infra-move" || parts[8] != "req" || ValidateSID(parts[4]) != nil || ValidateNID(parts[6]) != nil {
		return "", "", false
	}
	return parts[4], parts[6], true
}

// Both production consumers must subscribe directly to these exact subjects.
// Queue groups would let another host sign a receipt for an effect it never ran.
// This descriptor alone does not establish that production installed a SUB.
type InfraSubscription struct {
	Subject    string
	Subscriber string
	Publisher  string
}

func InfraSubscriptions(nodeID, sid, nid string) []InfraSubscription {
	var rows []InfraSubscription
	if ValidateClusterNodeID(nodeID) == nil {
		rows = append(rows, InfraSubscription{SubjClusterInfra(nodeID), "broker", "manager"})
	}
	if ValidateSID(sid) == nil && ValidateNID(nid) == nil {
		rows = append(rows, InfraSubscription{SubjInfraMove(sid, nid), "agent", "broker"})
	}
	return rows
}

// Shared by admin and health projections. Unknown external errors are opaque;
// free-form host errors may contain credentials or private filesystem paths.
func InfraCode(err error) string {
	if err == nil {
		return ""
	}
	for _, code := range []string{CodeInfraUnsupported, CodeInfraArtifactInvalid, CodeInfraConfigInvalid, CodeInfraIncompatible, CodeInfraPermissionDenied, CodeInfraPolicyStale, CodeInfraNotEnabled, CodeInfraBusy, CodeInfraNotReady, CodeInfraInventoryIncomplete, CodeInfraBudgetExceeded, CodeInfraResultUnknown, CodeInfraRecoveryRequired, CodeInfraInternal} {
		if err.Error() == code {
			return code
		}
	}
	return CodeInfraInternal
}

func InfraStatusReport(s c.Status) *InfraReport {
	phase := "unknown"
	if s.Phase >= c.Prepared && s.Phase <= c.Halted {
		phase = []string{"prepared", "staged", "ready", "moving", "cutting", "verifying", "returning", "retiring", "completed", "deferred", "halted"}[int(s.Phase)-1]
	}
	code := ""
	if s.Code != "" {
		code = InfraCode(errors.New(s.Code))
	}
	r := &InfraReport{Step: s.Step, Phase: phase, Revision: s.Revision, Code: code, LiveBusiness: s.Census.LiveBusiness, OldOwnerPending: s.Census.OldOwnerPending}
	if s.Tx != (c.ID{}) {
		r.TxID = hex.EncodeToString(s.Tx[:])
	}
	if s.Current.ExeSHA256 != (c.Digest{}) {
		r.RunningSHA256 = hex.EncodeToString(s.Current.ExeSHA256[:])
	}
	if s.Proof != (c.Digest{}) {
		r.Proof = hex.EncodeToString(s.Proof[:])
	}
	return r
}
