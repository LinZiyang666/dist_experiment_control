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
	// A pure render/replay leaf must never reach NATS: import the
	// transport-free half of xfercontract (identical identities).
	// origin: hot-update S7 plan §11.1 / P00 — NATS-free value split
	xfercontract "github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
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

// TransferRecordReqID preserves the content key used by existing Raft callers.
func TransferRecordReqID(rec schema.AuditTransfer) (string, error) {
	return schema.TransferRecordReqID(rec)
}

// CanonicalTransferAudit shares the exact schema normalization with history.
// origin: hot-update S7 plan §5.1–§5.2 / T23
func CanonicalTransferAudit(rec schema.AuditTransfer) (xfercontract.CanonicalRecord, error) {
	return schema.CanonicalTransferAudit(rec)
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
	aux, reqID, err := schema.TransferAuditPayload(rec)
	if err != nil {
		return nil, err
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
	// Retained outboxes re-submit these bytes after Aux truncation. Never
	// acknowledge a splice under the old record's content-addressed ReqID.
	// origin: hot-update S7 plan §5.2 / T23
	if schema.TransferContinuation(aux.Rec) {
		canonical, err := CanonicalTransferAudit(aux.Rec)
		if err != nil {
			return schema.AuditTransfer{}, false, err
		}
		if cmd.ReqID != canonical.ReqID {
			return schema.AuditTransfer{}, false, fmt.Errorf("xferaudit: continuation request identity conflict")
		}
	}
	return aux.Rec, true, nil
}
