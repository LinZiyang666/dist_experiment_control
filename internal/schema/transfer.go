package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	// The audit schema is read by NATS-free replay leaves (xferaudit): import
	// the transport-free half of xfercontract (identical identities).
	// origin: hot-update S7 plan §11.1 / P00 — NATS-free value split
	xfercontract "github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// AuditTransfer is a file-transfer lifecycle envelope.
// file-transfer-plan §Audit.
//
//	kind ∈ {"start","complete","failed"} — receiver-finalization invariant:
//	  start    written when broker accepts the prepare;
//	  complete written on agent ev.transfer (push) or ctl finalize.req (pull);
//	  failed   same parties, on the failure path or broker-side timeout.
//	verb ∈ {"push","pull"}.
//	tier ∈ {"a","b"} — "a" inline ≤ 8 MiB, "b" JetStream ObjectStore ≤ 2 GiB (v0.2.5+).
//
// Bytes / DurationMs are populated only on complete/failed; Bucket only
// on tier B; Code only on failed (machine-readable). The schema name
// matches the file-transfer-plan §Audit table verbatim — append-only.
type AuditTransfer struct {
	V          int       `json:"v"`
	Kind       string    `json:"kind"`
	Verb       string    `json:"verb"`
	Ts         time.Time `json:"ts"`
	Session    string    `json:"session"`
	Node       string    `json:"node"`
	ActorNkey  string    `json:"actor_nkey,omitempty"`
	ActorFp    string    `json:"actor_fp,omitempty"`
	TransferID string    `json:"transfer_id"`
	Path       string    `json:"path,omitempty"`
	Size       int64     `json:"size,omitempty"`
	SHA256     string    `json:"sha256,omitempty"`
	Tier       string    `json:"tier,omitempty"`
	Bucket     string    `json:"bucket,omitempty"`
	Bytes      int64     `json:"bytes,omitempty"`
	DurationMs int64     `json:"duration_ms,omitempty"`
	Code       string    `json:"code,omitempty"`
	Error      string    `json:"error,omitempty"`
	// Continuation identity binds replay to the original retained entry. Zero
	// values preserve legacy history bytes; any extension requires v1 validation.
	// origin: hot-update S7 plan §5.2 / T23
	AgentInstance string `json:"agent_instance,omitempty"`
	EntryNonce    string `json:"entry_nonce,omitempty"`
	IntentDigest  string `json:"intent_digest,omitempty"`
	ObjectVersion string `json:"object_version,omitempty"`
	TerminalID    string `json:"terminal_id,omitempty"`
	// Abandonment settles responsibility without claiming a known file outcome.
	// origin: hot-update S7 plan §6.4 / lead LQ1
	OutcomeUnknown bool `json:"outcome_unknown,omitempty"`
	// BestEffort marks a cap0 row whose current broker minted and durably
	// retained an admission nonce before forwarding, so its START and its one
	// TERMINAL slot reach exact history like v1. The old receiver protocol
	// grants no commit authority: such a row never carries agent, intent or
	// object identity and never certifies transfer-v1. Pre-S7 rows and v1 rows
	// omit it, so their history bytes are unchanged.
	// origin: hot-update S7 plan §4 / §5.2 / §9 / L12 (S7 integration oplegacy F2)
	BestEffort bool `json:"best_effort,omitempty"`
}

// TransferContinuation detects partial extensions too: dropping one identity
// field must not send a malformed v1 record through legacy bounded-loss handling.
// A best-effort cap0 row with a retained identity takes the same exact-history
// route; CanonicalTransferAudit keeps it from claiming v1 receiver authority.
// origin: hot-update S7 plan §5.2 / T23
func TransferContinuation(rec AuditTransfer) bool {
	return rec.AgentInstance != "" || rec.EntryNonce != "" || rec.IntentDigest != "" ||
		rec.ObjectVersion != "" || rec.TerminalID != "" || rec.OutcomeUnknown || rec.BestEffort
}

// BindLegacyTerminal stamps one decided cap0 terminal with the admission nonce
// its broker retained before forwarding. The terminal slot is the v1 formula,
// so a complete and a failed decision still compete for one slot. An empty
// nonce (no exact history owner at admission) leaves the pre-S7 row untouched:
// identity is retained from admission, never minted after an outcome.
// origin: hot-update S7 plan §4 / §5.1 / L12 (S7 integration oplegacy F2)
func BindLegacyTerminal(rec AuditTransfer, entryNonce string) AuditTransfer {
	if entryNonce != "" {
		rec.EntryNonce, rec.BestEffort = entryNonce, true
		rec.TerminalID = TransferTerminalID(rec.Session, rec.TransferID, entryNonce)
	}
	return rec
}

// TransferTerminalID fixes one terminal slot for granted and ungranted outcomes.
// It excludes kind so an opposite outcome cannot obtain a second terminal slot.
// origin: hot-update S7 plan §4 / T23
func TransferTerminalID(sid, transferID, entryNonce string) string {
	sum := sha256.Sum256([]byte("xfer-terminal-v1\n" + sid + "\n" + transferID + "\n" + entryNonce))
	return hex.EncodeToString(sum[:])
}

// TransferRecordReqID derives the cross-retry-STABLE idempotency key for one exact
// transfer-audit record. The full normalized record is included so a later legitimate
// transfer that reuses the same transfer_id/kind does not get dedup-skipped.
func TransferRecordReqID(rec AuditTransfer) (string, error) {
	rec.Ts = rec.Ts.Round(0)
	body, err := json.Marshal(rec)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("xferaudit-record:"), body...))
	return hex.EncodeToString(sum[:]), nil
}

// CanonicalTransferAudit is the single normalization boundary for ledger,
// publisher and history. Its digest covers the exact history bytes, while ReqID
// retains the existing content-addressed Raft key (no new op or command version).
// origin: hot-update S7 plan §5.1–§5.2 / T23
func CanonicalTransferAudit(rec AuditTransfer) (xfercontract.CanonicalRecord, error) {
	if err := proto.ValidateSID(rec.Session); err != nil {
		return xfercontract.CanonicalRecord{}, fmt.Errorf("xferaudit: invalid session: %w", err)
	}
	if rec.V != AuditSchemaVersion || rec.TransferID == "" || rec.Node == "" || rec.ActorFp == "" || rec.EntryNonce == "" ||
		rec.Ts.IsZero() || !rec.BestEffort && (rec.AgentInstance == "" || rec.IntentDigest == "") {
		return xfercontract.CanonicalRecord{}, fmt.Errorf("xferaudit: incomplete continuation identity")
	}
	if rec.BestEffort {
		// A cap0 row retains only the broker's admission nonce. Agent, intent or
		// object identity would certify receiver authority an old peer never
		// granted. Only a pull START keeps the unresolved tier it was admitted
		// with ("?"); its terminal names the tier the receiver actually used.
		// origin: hot-update S7 plan §4 / §9 / L12 (S7 integration oplegacy F2)
		if rec.AgentInstance != "" || rec.IntentDigest != "" || rec.ObjectVersion != "" ||
			(rec.Verb != "push" && rec.Verb != "pull") ||
			(rec.Tier != "a" && rec.Tier != "b" && (rec.Tier != "?" || rec.Verb != "pull" || rec.Kind != "start")) {
			return xfercontract.CanonicalRecord{}, fmt.Errorf("xferaudit: invalid best-effort cap0 identity")
		}
	} else if (rec.Verb != "push" && rec.Verb != "pull") || (rec.Tier != "a" && rec.Tier != "b") ||
		(rec.Tier == "b" && rec.ObjectVersion == "") {
		return xfercontract.CanonicalRecord{}, fmt.Errorf("xferaudit: invalid continuation intent")
	}
	switch rec.Kind {
	case "start":
		if rec.TerminalID != "" || rec.OutcomeUnknown {
			return xfercontract.CanonicalRecord{}, fmt.Errorf("xferaudit: start carries terminal outcome")
		}
	case "complete", "failed":
		if rec.TerminalID != TransferTerminalID(rec.Session, rec.TransferID, rec.EntryNonce) {
			return xfercontract.CanonicalRecord{}, fmt.Errorf("xferaudit: terminal identity conflict")
		}
		// An administrative abandonment cannot be represented as success, nor
		// can its reason be emitted while claiming the outcome is known.
		// origin: hot-update S7 plan §6.4 / lead LQ1
		if rec.OutcomeUnknown != (rec.Code == xfercontract.ReasonSettlementAbandoned) ||
			(rec.OutcomeUnknown && rec.Kind != "failed") {
			return xfercontract.CanonicalRecord{}, fmt.Errorf("xferaudit: invalid abandonment outcome")
		}
	default:
		return xfercontract.CanonicalRecord{}, fmt.Errorf("xferaudit: invalid continuation kind")
	}
	rec.Ts = rec.Ts.Round(0)
	body, err := json.Marshal(rec)
	if err != nil {
		return xfercontract.CanonicalRecord{}, fmt.Errorf("xferaudit: canonical bytes: %w", err)
	}
	reqID, err := TransferRecordReqID(rec)
	if err != nil {
		return xfercontract.CanonicalRecord{}, err
	}
	sum := sha256.Sum256(body)
	return xfercontract.CanonicalRecord{
		Identity: xfercontract.Identity{
			SID: rec.Session, NID: rec.Node, ActorFP: rec.ActorFp, AgentInstance: rec.AgentInstance,
			TransferID: rec.TransferID, EntryNonce: rec.EntryNonce, IntentDigest: rec.IntentDigest, ObjectVersion: rec.ObjectVersion,
		},
		Kind: rec.Kind, ReqID: reqID, Digest: hex.EncodeToString(sum[:]), Bytes: body,
	}, nil
}

// TransferAuditPayload preserves the legacy Aux envelope and normalized history
// record. Validation happens before Raft proposal, so partial v1 never becomes
// poison or silently falls back to legacy publication.
// origin: hot-update S7 plan §5.1–§5.2 / T23
func TransferAuditPayload(rec AuditTransfer) ([]byte, string, error) {
	if TransferContinuation(rec) {
		if _, err := CanonicalTransferAudit(rec); err != nil {
			return nil, "", err
		}
	}
	if rec.TransferID == "" {
		return nil, "", fmt.Errorf("xferaudit: empty transfer_id")
	}
	switch rec.Kind {
	case "start", "complete", "failed":
	default:
		return nil, "", fmt.Errorf("xferaudit: invalid kind %q (want start|complete|failed)", rec.Kind)
	}
	rec.Ts = rec.Ts.Round(0)
	reqID, err := TransferRecordReqID(rec)
	if err != nil {
		return nil, "", fmt.Errorf("xferaudit: reqid: %w", err)
	}
	aux, err := json.Marshal(struct {
		Rec AuditTransfer `json:"rec"`
	}{Rec: rec})
	if err != nil {
		return nil, "", fmt.Errorf("xferaudit: marshal aux: %w", err)
	}
	return aux, reqID, nil
}
