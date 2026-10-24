package xferserve

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infrarepair"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// LegacyConfirm is the canonical cap0 history writer bound to the broker's
// current JetStream: the v1 transfer-subject CAS and full readback, plus the
// receipt check the leader publisher applies, under one five-second bound and
// the same S9 AuditReplay permit as that publisher's exact path. A nil handle
// returns nil, so no admission retains an identity it cannot confirm.
// origin: hot-update S7 plan §5.2 / L12 (S7 integration oplegacy F2)
func LegacyConfirm(js jetstream.JetStream) func(schema.AuditTransfer) error {
	if js == nil {
		return nil
	}
	return func(rec schema.AuditTransfer) error {
		canonical, err := schema.CanonicalTransferAudit(rec)
		if err != nil {
			return err
		}
		d, err := controlbudget.After(5 * time.Second)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // ctx-none: the cap0 writer runs in NATS callbacks and ledger passes that hold no caller context.
		defer cancel()
		return infrarepair.RunLegacy(ctx, js, infrarepair.AuditReplay, func(context.Context) error {
			receipt, err := (sessionHistory{js: js}).Confirm(canonical, d)
			if err == nil && (receipt.Stream != jsstream.HistoryStreamName(rec.Session) ||
				receipt.Sequence == 0 || receipt.RecordDigest != canonical.Digest) {
				err = errors.New(x.CodeResultUnknown + ": cap0 history receipt does not name the exact row")
			}
			return err
		})
	}
}

// LegacyAudit owns exact cap0 terminal publication and bounded leader retry.
// origin: hot-update S7 plan §5.2 / P13
type LegacyAudit struct {
	Cluster  bool
	Forward  func([]byte) error
	Publish  func(string, []byte, string) error
	Logger   interface{ Warn(string, ...any) }
	Attempts int
	Backoff  time.Duration
}

// commitSyntheticTransferTerminal emits a #57 synthetic terminal and reports whether it
// durably COMMITTED. The caller deletes the ledger row only on true, so a false must
// always mean "not durable yet, try again next pass".
//
// TWO PATHS, because a single broker has no leader to forward to:
//
//   - CLUSTER (transferAuditForwardSync attached): forwards to the leader with the same
//     bounded transient-retry as the async sink, so a brief election is absorbed within
//     the pass. A persistent not_leader or a permanent error returns false. A sink that
//     is not attached YET (pre-wireClusterLate) also returns false — never delete a
//     ledger on an unconfirmed emit.
//   - SINGLE (b.cl == nil): publishes straight to the audit stream. There is no raft
//     dedup ledger here; what makes a replay safe is that the synthetic terminal is
//     content-deterministic, so JetStream collapses a repeat within its duplicate
//     window. See the branch's own comment for the limits of that argument.
//
// origin: prerelease audit round 2, C4. This doc described only the cluster path while
// the same commit added the single-mode branch beneath it — a branch that forwards
// nothing, retries nothing, and returns true with no sink attached.

func (h LegacyAudit) Commit(rec schema.AuditTransfer) bool {
	fwd := h.Forward
	if fwd == nil && h.Cluster {
		return false // sink not attached yet (pre-wireClusterLate) — retry next pass, never lose the ledger
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		h.Logger.Warn("broker: marshal synthetic transfer terminal", "err", err, "tid", rec.TransferID)
		return false
	}
	if fwd == nil {
		// SINGLE MODE. There is no raft dedup ledger here, and requiring one is why
		// the whole of #57 was inert on the deployment shape it was written for: the
		// sink is only ever attached by the cluster's AttachTransferAuditSink, so a
		// single broker could stage a synthetic terminal and never commit one.
		//
		// Published with a CONTENT-DERIVED Nats-Msg-Id, which is what actually makes a
		// replay safe.
		//
		// origin: prerelease audit round 2, F-T2. This used to say the publish was safe
		// because the terminal is byte-deterministic and "JetStream's own duplicate
		// window collapses it". That is false: JetStream dedups on the Nats-Msg-Id
		// header and NEVER on payload bytes, and publishAudit set no header — so a
		// replay produced a SECOND identical terminal. internal/jsstream/replicas.go
		// documents that exact fact two files over, and the repo's own recovery test
		// caught the duplicate the moment F-T1 made single-mode replay reachable.
		//
		// TransferRecordReqID hashes the whole normalized record, so the id is stable
		// across retries and distinct for a genuinely different terminal.
		msgID, ierr := xferaudit.TransferRecordReqID(rec)
		if ierr != nil {
			h.Logger.Warn("broker: cannot derive a dedup id for a single-mode transfer terminal — "+
				"retaining the ledger rather than risking a duplicate", "err", ierr, "tid", rec.TransferID)
			return false
		}
		// WITH a content-derived Nats-Msg-Id. origin: round 2, F-T2 — the comment that
		// used to justify this publish claimed JetStream collapses byte-identical
		// payloads. It does not; it dedups on this header and publishAudit sets none.
		if perr := h.Publish(proto.SubjAuditTransfer(rec.Session), payload, msgID); perr != nil {
			h.Logger.Warn("broker: single-mode synthetic transfer terminal publish failed (retry next pass)",
				"err", perr, "tid", rec.TransferID)
			return false
		}
		return true
	}
	for attempt := 0; attempt < h.Attempts; attempt++ {
		ferr := fwd(payload)
		if ferr == nil {
			return true // committed (content reqID dedups any earlier partial attempt)
		}
		if !errors.Is(ferr, cluster.ErrForwardNotLeader) {
			h.Logger.Warn("broker: synthetic transfer terminal forward failed (retry next pass)", "err", ferr, "tid", rec.TransferID)
			return false
		}
		time.Sleep(h.Backoff)
	}
	return false // persistent not_leader — retry on the next pass
}
