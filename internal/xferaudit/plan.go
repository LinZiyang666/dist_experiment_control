// Package xferaudit renders + replays the re-derivable transfer-audit op
// (D8a, architecture §9/§6.3). A transfer lifecycle event (start/complete/failed)
// becomes ONE committed OpTransferAudit entry whose apply-inert Command.Aux carries
// the full schema.AuditTransfer record; the D5 audit publisher replays it PURELY from
// the committed bytes (no live request, no DB), exactly as proc.ReplayReconcileAudit
// does for OpReconcileBatch. This leaf — not internal/cluster — owns schema.AuditTransfer
// so the FSM core stays free of the audit schema (L-2; the op rides genericExecApplier
// with an EMPTY Body — transfer audit lives only in the JS history-<sid> stream, never a
// SQL row, so there is nothing to mutate).
package xferaudit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/schema"
)

// xferAuxV1 is the apply-inert Command.Aux payload for OpTransferAudit: the single
// audit record this entry stands for. genericExecApplier ignores Aux; only
// ReplayTransferAudit reads it.
type xferAuxV1 struct {
	Rec schema.AuditTransfer `json:"rec"`
}

// TransferReqID is the legacy coarse key for a transfer lifecycle slot.
func TransferReqID(transferID, kind string) string {
	sum := sha256.Sum256([]byte("xferaudit:" + transferID + ":" + kind))
	return hex.EncodeToString(sum[:])
}

// TransferRecordReqID derives the cross-retry-STABLE idempotency key for one exact
// transfer-audit record. The full normalized record is included so a later legitimate
// transfer that reuses the same transfer_id/kind does not get dedup-skipped.
func TransferRecordReqID(rec schema.AuditTransfer) (string, error) {
	rec.Ts = rec.Ts.Round(0)
	body, err := json.Marshal(rec)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("xferaudit-record:"), body...))
	return hex.EncodeToString(sum[:]), nil
}

// PlanTransferAudit renders OpTransferAudit for one audit record: EMPTY Body (no DB
// mutation), Aux = the record, ReqID = TransferRecordReqID(rec) — the CONTENT-addressed
// key. Batch-A A4: this line used to name TransferReqID, the COARSE (transferID, kind)
// key. That misreads as "dedup is per-transfer-and-kind", which would let a reviewer
// accept a re-emit that must in fact be distinguished by content — the #57
// finalize-on-recovery design rests on the content key. The Ts monotonic reading is
// stripped (rec.Ts.Round(0)) so the replayed JSON is byte-identical to the live
// pubAuditTransfer payload (time.Time.MarshalJSON already drops the monotonic part; this
// is the same defensive strip proc.PlanReconcileBatch applies). rec.Kind must be one of
// start/complete/failed and rec.TransferID non-empty — a malformed record is a leader-side
// programming error, rejected here so it never becomes a poison entry.
func PlanTransferAudit(rec schema.AuditTransfer) (*cluster.Command, error) {
	if rec.TransferID == "" {
		return nil, fmt.Errorf("xferaudit: empty transfer_id")
	}
	switch rec.Kind {
	case "start", "complete", "failed":
	default:
		return nil, fmt.Errorf("xferaudit: invalid kind %q (want start|complete|failed)", rec.Kind)
	}
	rec.Ts = rec.Ts.Round(0)
	reqID, err := TransferRecordReqID(rec)
	if err != nil {
		return nil, fmt.Errorf("xferaudit: reqid: %w", err)
	}
	aux, err := json.Marshal(xferAuxV1{Rec: rec})
	if err != nil {
		return nil, fmt.Errorf("xferaudit: marshal aux: %w", err)
	}
	cmd := cluster.NewCommand(cluster.OpTransferAudit) // empty Body — apply is a deterministic no-op
	cmd.Aux = aux
	cmd.ReqID = reqID
	return cmd, nil
}

// ReplayTransferAudit deterministically reconstructs the single AuditTransfer record a
// committed OpTransferAudit entry stands for, reading ONLY cmd.Aux — no live request, no
// leader-local map, no DB (§6.3). Two leaders replaying the SAME committed bytes return
// the identical record. ok=false on an empty Aux (yields no published row); a non-
// OpTransferAudit command is a caller error.
func ReplayTransferAudit(cmd *cluster.Command) (rec schema.AuditTransfer, ok bool, err error) {
	if cmd == nil || cmd.Op != cluster.OpTransferAudit {
		return schema.AuditTransfer{}, false, fmt.Errorf("xferaudit: ReplayTransferAudit: not a TransferAudit command")
	}
	if len(cmd.Aux) == 0 {
		return schema.AuditTransfer{}, false, nil
	}
	var aux xferAuxV1
	if err := json.Unmarshal(cmd.Aux, &aux); err != nil {
		return schema.AuditTransfer{}, false, fmt.Errorf("xferaudit: decode aux: %w", err)
	}
	return aux.Rec, true, nil
}
