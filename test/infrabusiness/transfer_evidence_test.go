//go:build linux && infrabusiness_integration

package infrabusiness_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// All sample times use the observer's single boot clock, including observations
// of remote hosts. Host BOOTTIME values must never be compared across machines.
type durableCheckpoint struct {
	BootID        string
	AtNS, Durable int64
	PrefixSHA256  string
	Synced        bool
}
type consumerRetirement struct {
	Consumer, Stream          string
	UnsubNS, PONGNS, DeleteNS int64
	Absent                    bool
}
type transferFinalReceipt struct {
	Checkpoints        []durableCheckpoint
	Original           businessIdentity
	BootID             string
	CompletedNS, Bytes int64
	SHA256             string
	History            x.HistorySet
	Consumers          []consumerRetirement
	ReceiptEvidence    string
}
type phaseRequirement struct {
	Consumer string
	Phase    string
	Original businessIdentity
}
type transferPhaseState struct {
	BootID                           string
	AtNS, Written, Synced            int64
	Published, ACKed                 uint64
	Renamed, ParentSynced, Delivered bool
	Terminal                         x.CanonicalRecord
}
type transferPhaseReceipt struct {
	evidenceScope
	Step            uint32
	Phase           string
	Original        businessIdentity
	Before, After   transferPhaseState
	Final           transferFinalReceipt
	ReceiptEvidence string
}

func contentDigest(v any) string {
	body, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return digest(body)
}
func finalDigest(row transferFinalReceipt) string {
	row.ReceiptEvidence = ""
	return contentDigest(row)
}
func phaseDigest(row transferPhaseReceipt) string {
	row.ReceiptEvidence = ""
	return contentDigest(row)
}
func transferDigest(row transferReceipt) string {
	row.ReceiptEvidence = ""
	return contentDigest(row)
}

// origin: S9 internal review rv-business F5
// The exact four tuples are frozen before observing any hop. Counting push and
// pull totals lets both directions silently omit one of the required sizes.
func transferOriginals(r businessReceipt) (map[string]businessIdentity, error) {
	originals := make(map[string]businessIdentity)
	var sealed []businessIdentity
	quadrants, sessions, buckets := make(map[string]bool), make(map[string]bool), make(map[string]bool)
	for _, id := range r.Required {
		if id.Role != "transfer" {
			continue
		}
		quadrant := fmt.Sprintf("%d/%s", id.Size, id.Direction)
		if id.Size != 66549440 && id.Size != 1<<30 || id.Direction != "push" && id.Direction != "pull" || quadrants[quadrant] ||
			id.TransferID == "" || id.Session == "" || id.Bucket == "" || sessions[id.Session] || buckets[id.Bucket] || originals[id.TransferID].TransferID != "" ||
			!validDigest(id.SHA256) || id.AuditIdentity.SID != id.Session || id.AuditIdentity.TransferID != id.TransferID || id.AuditIdentity.ObjectVersion != id.NUID ||
			id.AuditIdentity.EntryNonce != id.Nonce {
			return nil, errors.New("frozen transfer size/direction/session/bucket quadrant is missing or duplicated")
		}
		budget, err := proto.TransferBudget("b", id.Size, id.RateLimitBPS)
		if err != nil || id.RateLimitBPS <= 0 || id.StartedNS <= 0 || id.DeadlineNS-id.StartedNS != int64(budget) {
			return nil, errors.New("frozen transfer pacing has no valid original product budget")
		}
		sealed = append(sealed, id)
		originals[id.TransferID], quadrants[quadrant], sessions[id.Session], buckets[id.Bucket] = id, true, true, true
	}
	if len(originals) != 4 {
		return nil, errors.New("four original transfer quadrants are required")
	}
	if len(r.Hops) == 0 || r.TransferSealNS <= 0 || r.TransferSealNS > r.Hops[0].StartNS || r.TransferSealSHA256 != contentDigest(sealed) {
		return nil, errors.New("transfer tuples were not sealed before the first cut")
	}
	for _, id := range sealed {
		if r.TransferSealNS < id.StartedNS {
			return nil, errors.New("transfer seal predates an original invocation")
		}
	}
	return originals, nil
}

// origin: S9 internal review rv-business F4
// Check the raw durable series before considering summaries. Each hop must
// recover forward progress; time since the last advance includes both edges of
// a fault and gaps between hops, even when the cached MaxSilenceNS says zero.
func checkTransferProgress(r businessReceipt, originals map[string]businessIdentity) error {
	hops, err := scopedHops(r)
	if err != nil {
		return err
	}
	if r.ObserverBootID == "" {
		return errors.New("transfer observer boot is missing")
	}
	type key struct {
		step      uint32
		phase, id string
	}
	rows := make(map[key]transferReceipt)
	for _, row := range r.Transfers {
		h, ok := hops[row.Step]
		id := originals[row.TransferID]
		if !ok || row.evidenceScope != scopeFor(h, row.Phase) || row.Direction != id.Direction || row.Session != id.Session || row.Bucket != id.Bucket ||
			row.ReceiptEvidence != transferDigest(row) || len(row.Checkpoints) == 0 {
			return errors.New("transfer evidence is outside the frozen cut tuple or digest")
		}
		k := key{row.Step, row.Phase, row.TransferID}
		if _, exists := rows[k]; exists {
			return errors.New("duplicate transfer checkpoint lane")
		}
		rows[k] = row
	}
	for id := range originals {
		var last durableCheckpoint
		var lastAdvance int64
		initialized := false
		for _, h := range r.Hops {
			var before int64
			for _, phase := range []string{"before", "moving", "returning"} {
				row, ok := rows[key{h.Step, phase, id}]
				if !ok {
					return errors.New("raw checkpoint phase is missing")
				}
				if initialized && row.PreviousDurable != last.Durable {
					return errors.New("raw checkpoint lost the previous durable offset")
				}
				for _, cp := range row.Checkpoints {
					if cp.BootID != r.ObserverBootID || cp.AtNS < 0 || cp.Durable <= 0 || cp.Durable >= row.Size || !cp.Synced || !validDigest(cp.PrefixSHA256) ||
						initialized && (cp.AtNS < last.AtNS || cp.Durable < last.Durable || cp.AtNS == last.AtNS && cp.Durable != last.Durable) {
						return errors.New("raw durable checkpoint changed boot, order or synced identity")
					}
					if !initialized {
						lastAdvance, initialized = cp.AtNS, true
					}
					if cp.AtNS-lastAdvance > int64(upgradebudget.TransferProgress) {
						return errors.New("raw durable progress exceeded the fault-window stall budget")
					}
					if cp.Durable > last.Durable {
						lastAdvance = cp.AtNS
					}
					last = cp
				}
				if last.Durable != row.Durable || last.PrefixSHA256 != row.RawPrefixSHA256 {
					return errors.New("summary contradicts the raw durable checkpoint")
				}
				first := row.Checkpoints[0]
				switch phase {
				case "before":
					if first.AtNS > h.StartNS || last.AtNS > h.CutNS {
						return errors.New("raw checkpoints omit the fault leading edge")
					}
					before = last.Durable
				case "moving":
					if first.AtNS < h.StartNS || last.AtNS > h.EndNS {
						return errors.New("raw moving checkpoint is outside the fault window")
					}
				case "returning":
					if first.AtNS < h.CutNS || last.AtNS < h.EndNS || last.Durable <= before {
						return errors.New("transfer never resumed durable progress through this hop")
					}
				}
			}
		}
		// Keep the same last-advance clock through final drain. A last-hop
		// sample with unchanged bytes cannot reset the final stall window.
		for _, final := range r.TransferFinals {
			if final.Original.TransferID != id {
				continue
			}
			for _, cp := range final.Checkpoints {
				if cp.BootID != r.ObserverBootID || !cp.Synced || !validDigest(cp.PrefixSHA256) || cp.AtNS < last.AtNS || cp.Durable < last.Durable || cp.Durable > originals[id].Size || cp.AtNS == last.AtNS && cp.Durable != last.Durable || cp.AtNS-lastAdvance > int64(upgradebudget.TransferProgress) {
					return errors.New("final raw progress changed identity or exceeded the stall budget")
				}
				if cp.Durable > last.Durable {
					lastAdvance = cp.AtNS
				}
				last = cp
			}
		}
	}
	return nil
}

// origin: S9 internal review rv-business F4
func checkCanonical(record x.CanonicalRecord, id businessIdentity, kind string) error {
	var audit schema.AuditTransfer
	if json.Unmarshal(record.Bytes, &audit) != nil {
		return errors.New("transfer audit bytes are missing")
	}
	canonical, err := schema.CanonicalTransferAudit(audit)
	if err != nil || !reflect.DeepEqual(canonical, record) || record.Identity != id.AuditIdentity || record.Identity.SID != id.Session || record.Identity.TransferID != id.TransferID || record.Identity.EntryNonce != id.Nonce || record.Identity.ObjectVersion != id.NUID || record.Kind != kind ||
		audit.Verb != id.Direction || audit.Bucket != id.Bucket || audit.Size != id.Size || audit.SHA256 != id.SHA256 || audit.OutcomeUnknown ||
		kind == "complete" && (audit.Bytes != id.Size || audit.Code != "") {
		return errors.New("canonical START/TERMINAL content differs from the original transfer")
	}
	return nil
}

// origin: S9 internal review rv-business F4
// Final SHA is one conjunct: complete history must contain exactly the original
// START and successful TERMINAL, and their actual stream receipts must match.
func checkTransferFinal(row transferFinalReceipt, id businessIdentity, boot string, after int64, consumers map[string]bool) error {
	if len(row.Checkpoints) == 0 {
		return errors.New("final raw durable checkpoint is missing")
	}
	last := row.Checkpoints[len(row.Checkpoints)-1]
	if last.BootID != boot || !last.Synced || last.AtNS != row.CompletedNS || last.Durable != id.Size || last.PrefixSHA256 != id.SHA256 {
		return errors.New("final raw checkpoint contradicts the final file")
	}
	if row.Original != id || row.BootID != boot || row.CompletedNS < after || row.CompletedNS > id.DeadlineNS || row.Bytes != id.Size || row.SHA256 != id.SHA256 ||
		row.ReceiptEvidence != finalDigest(row) || !row.History.Complete || len(row.History.Start) != 1 || len(row.History.Terminal) != 1 || len(row.History.Receipts) != 2 {
		return errors.New("original final byte/hash/history receipt is incomplete")
	}
	records := []x.CanonicalRecord{row.History.Start[0], row.History.Terminal[0]}
	for n, kind := range []string{"start", "complete"} {
		if err := checkCanonical(records[n], id, kind); err != nil {
			return err
		}
		receipt := row.History.Receipts[n]
		if receipt.Stream != jsstream.HistoryStreamName(id.Session) || receipt.Sequence == 0 || receipt.Sequence > row.History.ScannedThrough || receipt.RecordDigest != records[n].Digest ||
			n == 1 && receipt.Sequence <= row.History.Receipts[0].Sequence {
			return errors.New("canonical transfer history lacks exact stream custody")
		}
	}
	seen := make(map[string]bool)
	for _, c := range row.Consumers {
		if c.Consumer == "" || seen[c.Consumer] || !consumers[c.Consumer] || c.Stream != "OBJ_"+id.Bucket || !c.Absent ||
			c.UnsubNS <= 0 || c.PONGNS < c.UnsubNS || c.DeleteNS < c.PONGNS || c.DeleteNS > row.CompletedNS {
			return errors.New("final consumer retirement lacks UNSUB/PONG/delete/absence proof")
		}
		seen[c.Consumer] = true
	}
	if len(seen) != len(consumers) || len(seen) == 0 {
		return errors.New("final consumer retirement set is incomplete")
	}
	return nil
}

// origin: S9 internal review rv-business F4
func checkTransferFinals(r businessReceipt, originals map[string]businessIdentity) error {
	if len(r.TransferFinals) != len(originals) {
		return errors.New("one final receipt per original transfer is required")
	}
	var end int64
	for _, h := range r.Hops {
		end = max(end, h.EndNS)
	}
	seen := make(map[string]bool)
	for _, final := range r.TransferFinals {
		id, ok := originals[final.Original.TransferID]
		if !ok || seen[id.TransferID] {
			return errors.New("final receipt replaced an original invocation")
		}
		consumers := make(map[string]bool)
		for _, row := range r.Transfers {
			if row.TransferID == id.TransferID {
				consumers[row.Consumer] = true
				if row.RetiredConsumer != "" {
					consumers[row.RetiredConsumer] = true
				}
			}
		}
		if err := checkTransferFinal(final, id, r.ObserverBootID, end, consumers); err != nil {
			return err
		}
		seen[id.TransferID] = true
	}
	return nil
}

// origin: S9 internal review rv-business F4
// Payload phase records refer to the original invocations. Commit and courier
// need independent invocations because the original four must remain live.
// Inspect actual cross-cut states, not Coverage labels, and bind the terminal
// courier to the canonical history bytes.
func checkTransferPhases(r businessReceipt, originals map[string]businessIdentity) error {
	expected := map[string]bool{"writer-pending-ack": false, "reader-partial-write": false, "reader-synced": false, "commit-rename": false, "terminal-courier": false}
	if len(r.RequiredPhases) != len(expected) || len(r.TransferPhases) != len(expected) {
		return errors.New("independent phase invocation set is incomplete")
	}
	required := make(map[string]businessIdentity)
	requiredConsumers := make(map[string]string)
	ids, sessions, buckets := make(map[string]bool), make(map[string]bool), make(map[string]bool)
	for _, id := range originals {
		ids[id.TransferID], sessions[id.Session], buckets[id.Bucket] = true, true, true
	}
	for _, lane := range r.RequiredPhases {
		id := lane.Original
		_, known := expected[lane.Phase]
		original, payload := originals[id.TransferID]
		if payload && (id != original || lane.Phase == "commit-rename" || lane.Phase == "terminal-courier") {
			return errors.New("terminal phase replaced a still-live original transfer")
		}
		if !known || required[lane.Phase].TransferID != "" || id.TransferID == "" || !payload && (ids[id.TransferID] || sessions[id.Session] || buckets[id.Bucket] || lane.Consumer == "") ||
			id.Session == "" || id.Bucket == "" || id.CTLPID <= 0 || id.CTLStartTicks == 0 || id.Size <= 0 || !validDigest(id.SHA256) {
			return errors.New("phase invocation is not independently frozen")
		}
		required[lane.Phase], ids[id.TransferID], sessions[id.Session], buckets[id.Bucket] = id, true, true, true
		requiredConsumers[lane.Phase] = lane.Consumer
	}
	hops, err := scopedHops(r)
	if err != nil {
		return err
	}
	for _, lane := range r.TransferPhases {
		h, ok := hops[lane.Step]
		id := required[lane.Phase]
		a, b := lane.Before, lane.After
		if !ok || id != lane.Original || expected[lane.Phase] || lane.evidenceScope != scopeFor(h, lane.Phase) || lane.ReceiptEvidence != phaseDigest(lane) ||
			a.BootID != r.ObserverBootID || b.BootID != a.BootID || a.AtNS < h.StartNS || a.AtNS >= h.CutNS || b.AtNS <= h.CutNS || b.AtNS > h.EndNS ||
			a.Written < 0 || a.Synced < 0 || a.Synced > a.Written || b.Written < a.Written || b.Synced < a.Synced || b.Synced > b.Written || b.Written > id.Size {
			return errors.New("phase lane has no independent cross-cut state evidence")
		}
		switch lane.Phase {
		case "writer-pending-ack":
			if a.Published <= a.ACKed || b.ACKed < a.Published {
				return errors.New("phase did not preserve an actual pending publish ACK")
			}
		case "reader-partial-write":
			if a.Written <= a.Synced || b.Synced < a.Written {
				return errors.New("phase did not preserve a partial sink write")
			}
		case "reader-synced":
			if a.Synced <= 0 || b.Synced <= a.Synced {
				return errors.New("phase did not continue an already synced sink")
			}
		case "commit-rename":
			if a.Synced != id.Size || a.Renamed || !b.Renamed || !b.ParentSynced {
				return errors.New("phase did not cross commit/rename with parent sync")
			}
		case "terminal-courier":
			if checkCanonical(a.Terminal, id, "complete") != nil || a.Delivered || !b.Delivered || !reflect.DeepEqual(a.Terminal, b.Terminal) ||
				len(lane.Final.History.Terminal) != 1 || !bytes.Equal(b.Terminal.Bytes, lane.Final.History.Terminal[0].Bytes) {
				return errors.New("phase did not deliver the same canonical terminal")
			}
		default:
			return errors.New("unknown transfer phase")
		}
		if _, payload := originals[id.TransferID]; payload {
			// The four originals settle once in TransferFinals. A phase lane
			// cannot furnish a second final decision for the same invocation.
			if !reflect.DeepEqual(lane.Final, transferFinalReceipt{}) {
				return errors.New("payload phase supplied a second final receipt")
			}
		} else {
			consumers := map[string]bool{requiredConsumers[lane.Phase]: true}
			if err := checkTransferFinal(lane.Final, id, r.ObserverBootID, b.AtNS, consumers); err != nil {
				return err
			}
		}
		expected[lane.Phase] = true
	}
	for _, present := range expected {
		if !present {
			return errors.New("actual cross-cut transfer phase is missing")
		}
	}
	return nil
}
