package infrarepair

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats.go/jetstream"
)

type prefixChunk struct {
	Index  uint64 `json:"index"`
	Length uint64 `json:"length"`
	SHA256 string `json:"sha256"`
}

type originalPrefix struct {
	acks     []ACK
	expected []prefixChunk
}

type prefixObservation struct {
	TransferID     string        `json:"transfer_id"`
	AtNS           uint64        `json:"at_ns"`
	BeforeRepairNS uint64        `json:"before_repair_ns"`
	Expected       []prefixChunk `json:"expected"`
	Actual         []prefixChunk `json:"actual"`
}

// Capture reads the original server-ACK sequence while Seal owns producer
// admission. The original Message digest authorizes the expected byte hash;
// neither a later repaired read nor StreamInfo.LastSeq can invent it. Transfer
// identity is explicit because object NUID and invocation ID are distinct.
// Only public chunk lengths and hashes are retained, never message payloads.
// origin: hot-update S9 plan §4.3 / T40 / M20; s9-instr4 Request 2
func (g *Gate) captureOriginalPrefixes(receipts map[string]Responsibilities, d c.Deadline) ([]originalPrefix, error) {
	if drillobserve.Hook == nil {
		return nil, nil
	}
	byTransfer := make(map[string][]ACK)
	count := 0
	for _, r := range receipts {
		for _, ack := range r.ACKs {
			if ack.TransferID == "" {
				continue
			}
			count++
			// The four-MiB public codec has a finite complete-prefix inventory.
			// Refuse rather than dropping old chunks or silently truncating it.
			if len(ack.TransferID) > 128 || ack.NUID == "" || count > 32768 {
				return nil, c.ErrIncomplete
			}
			byTransfer[ack.TransferID] = append(byTransfer[ack.TransferID], ack)
		}
	}
	ids := make([]string, 0, len(byTransfer))
	for id := range byTransfer {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ctx, finish, err := requestContext(d, nil)
	if err != nil {
		return nil, err
	}
	defer finish()
	prefixes := make([]originalPrefix, 0, len(ids))
	for _, id := range ids {
		acks := byTransfer[id]
		sort.Slice(acks, func(i, j int) bool { return acks[i].Index < acks[j].Index })
		p := originalPrefix{acks: acks, expected: make([]prefixChunk, 0, len(acks))}
		for index, ack := range acks {
			if ack.Index != uint64(index) || ack.Account != acks[0].Account || ack.Stream != acks[0].Stream || ack.NUID != acks[0].NUID {
				return nil, c.ErrIncomplete
			}
			stream, err := g.cfg.Accounts[ack.Account].Stream(ctx, ack.Stream)
			if err != nil {
				return nil, readError(err)
			}
			msg, err := storedMessage(ctx, g.cfg.Accounts[ack.Account], stream, ack.Sequence)
			if err != nil {
				return nil, readError(err)
			}
			if !matchesACK(ack, msg) {
				return nil, ErrReceipt
			}
			p.expected = append(p.expected, observedChunk(ack.Index, msg.Data))
		}
		prefixes = append(prefixes, p)
	}
	return prefixes, admission(d, nil)
}

func observedChunk(index uint64, data []byte) prefixChunk {
	digest := sha256.Sum256(data)
	return prefixChunk{index, uint64(len(data)), hex.EncodeToString(digest[:])}
}

func matchesACK(ack ACK, msg *jetstream.RawStreamMsg) bool {
	return msg.Sequence == ack.Sequence && msg.Subject == ack.Subject && MessageDigest(msg.Header, msg.Data) == ack.Message
}

// This runs inside VerifyBeforeReplay's operation lease. Release cannot reopen
// repair until every row has crossed the bounded custody queue. A missing or
// changed chunk emits the actual incomplete prefix and returns the same sticky
// receipt loss as the product verifier; eventual repair cannot overwrite it.
func (g *Gate) verifyOriginalPrefixes(d c.Deadline, stop c.Stop) error {
	if len(g.prefixes) == 0 {
		return nil
	}
	ctx, finish, err := requestContext(d, stop)
	if err != nil {
		return err
	}
	defer finish()
	var loss error
	rows := make([]prefixObservation, 0, len(g.prefixes))
	for _, p := range g.prefixes {
		row := prefixObservation{TransferID: p.acks[0].TransferID, Expected: p.expected, Actual: make([]prefixChunk, 0, len(p.acks))}
		for _, ack := range p.acks {
			if err := admission(d, stop); err != nil {
				return err
			}
			stream, err := g.cfg.Accounts[ack.Account].Stream(ctx, ack.Stream)
			if err != nil {
				return readError(err)
			}
			msg, err := storedMessage(ctx, g.cfg.Accounts[ack.Account], stream, ack.Sequence)
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				loss = ErrReceipt
				continue
			}
			if err != nil {
				return readError(err)
			}
			row.Actual = append(row.Actual, observedChunk(ack.Index, msg.Data))
			if !matchesACK(ack, msg) {
				loss = ErrReceipt
			}
		}
		at, err := controlbudget.Now()
		if err != nil {
			return err
		}
		row.AtNS = uint64(at)
		rows = append(rows, row)
	}
	before, err := controlbudget.Now()
	if err != nil {
		return err
	}
	for _, row := range rows {
		row.BeforeRepairNS = uint64(before)
		data, err := json.Marshal(row)
		if err != nil {
			return c.ErrIncomplete
		}
		if err := drillobserve.RecordInfrastructure(drillobserve.InfrastructurePrefixKind, g.plan.Tx, g.plan.Step, data); err != nil {
			return err
		}
	}
	return loss
}
