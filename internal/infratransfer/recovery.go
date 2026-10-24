package infratransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"slices"
	"strconv"
	"sync"

	i "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrarepair"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// RecoveryWriterJS registers one original invocation BEFORE constructing its
// S7 writer. Both the initial writer and DataCurrent must use this same port:
// replacing it at reconnect would lose the original ACK ledger. The caller
// supplies transport retirement; a timeout cannot be cleared by a store read.
// No source bytes or credentials are retained in this memory-only ledger.
// origin: hot-update S9 plan §4.3 / T40 / T42; NC-EXPORTS Request P18/P19
func RecoveryWriterJS(js jetstream.JetStream, spec x.ObjectSpec, gate *infrarepair.Gate, account string, retire func(i.Deadline, i.Stop) error) (jetstream.JetStream, error) {
	if js == nil || gate == nil || account == "" || retire == nil || xferbucket.ValidateObjectSpec(spec) != nil {
		return nil, i.ErrIncomplete
	}
	w := &recoveryWriterJS{JetStream: js, gate: gate, spec: spec, account: account, name: "transfer:" + spec.Identity.TransferID, ledger: &writerACKs{pending: make(map[uint64]infrarepair.ACK)}}
	err := gate.Register(infrarepair.Producer{Name: w.name, Retire: retire, Capture: w.capture,
		Check: func(infrarepair.Responsibilities, i.Deadline, i.Stop) error { return nil }})
	if err != nil {
		return nil, err
	}
	return w, nil
}

type recoveryWriterJS struct {
	jetstream.JetStream
	gate          *infrarepair.Gate
	spec          x.ObjectSpec
	account, name string
	ledger        *writerACKs
}

type writerACKs struct {
	mu      sync.Mutex
	acks    []infrarepair.ACK
	pending map[uint64]infrarepair.ACK
}

// RecoveryWriterCurrent keeps the registered original journal while replacing
// only the JS transport. Pass it to WrapWriter; registration must not be repeated
// for a successor NC, nor may its prior receipts be rebuilt from that NC's store.
func RecoveryWriterCurrent(port jetstream.JetStream, current DataCurrent) (DataCurrent, error) {
	w, ok := port.(*recoveryWriterJS)
	if !ok || w == nil || current == nil {
		return nil, i.ErrIncomplete
	}
	return func(d i.Deadline) (jetstream.JetStream, error) {
		next, err := current(d)
		if err != nil {
			return nil, err
		}
		if next == nil {
			return nil, i.ErrIncomplete
		}
		return &recoveryWriterJS{JetStream: next, gate: w.gate, spec: w.spec, account: w.account, name: w.name, ledger: w.ledger}, nil
	}, nil
}

func (w *recoveryWriterJS) capture(i.Deadline) (infrarepair.Responsibilities, error) {
	w.ledger.mu.Lock()
	defer w.ledger.mu.Unlock()
	return infrarepair.Responsibilities{ACKs: append([]infrarepair.ACK(nil), w.ledger.acks...)}, nil
}

// The permit joins the actual PublishMsg and receipt insertion. Seal therefore
// cannot overtake an ACK that has reached the writer but not its ledger. Only a
// definite original ACK is retained. S7 resolves a lost reply through the
// independently checked original read below; transport retirement stays separate.
// Metadata takes the same fence but cannot masquerade as a chunk prefix.
func (w *recoveryWriterJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (ack *jetstream.PubAck, err error) {
	if msg == nil {
		return nil, i.ErrIncomplete
	}
	permit, err := w.gate.Enter(w.name)
	if err != nil {
		return nil, err
	}
	outcome := error(infrarepair.ErrUnknown)
	defer func() { permit.Finish(outcome) }()
	// Clone before IO; recording a caller-mutated header after ACK would bind
	// another request. The exact original message digest is never a LastSeq.
	original := &nats.Msg{Subject: msg.Subject, Data: bytes.Clone(msg.Data), Header: make(nats.Header, len(msg.Header))}
	for key, values := range msg.Header {
		original.Header[key] = slices.Clone(values)
	}
	ack, err = w.JetStream.PublishMsg(ctx, original, opts...)
	if err != nil {
		// Retain only the original request's public digest, never its bytes.
		// A later S7 read can prove this precise effect; LastSeq cannot.
		if original.Subject == xferbucket.ChunkSubject(w.spec) {
			if row, e := w.chunkACK(original, 0); e == nil {
				w.ledger.mu.Lock()
				w.ledger.pending[row.Index] = row
				w.ledger.mu.Unlock()
			}
		}
		outcome = err
		return nil, err
	}
	if ack == nil || ack.Stream != w.spec.Stream || ack.Sequence == 0 {
		return nil, infrarepair.ErrReceipt
	}
	if original.Subject == xferbucket.ChunkSubject(w.spec) {
		row, e := w.chunkACK(original, ack.Sequence)
		if e != nil {
			return nil, e
		}
		err = w.remember(row)
	}
	if err == nil {
		outcome = nil
	}
	return ack, err
}

func (w *recoveryWriterJS) chunkACK(original *nats.Msg, sequence uint64) (infrarepair.ACK, error) {
	index, e := strconv.ParseUint(original.Header.Get("Tether-Xfer-Index"), 10, 32)
	r := x.ChunkReceipt{Index: uint32(index), StreamSeq: sequence, Offset: int64(index) * int64(w.spec.ChunkSize), Length: uint32(len(original.Data)), SHA256: sha256.Sum256(original.Data)}
	if e != nil || index >= uint64(w.spec.Chunks) || r.Length != xferbucket.ChunkLength(w.spec, uint32(index)) ||
		xferbucket.CheckChunk(w.spec, r, &jetstream.RawStreamMsg{Subject: original.Subject, Sequence: sequence, Header: original.Header, Data: original.Data}) != nil {
		return infrarepair.ACK{}, infrarepair.ErrReceipt
	}
	return infrarepair.ACK{TransferID: w.spec.Identity.TransferID, Account: w.account, Stream: w.spec.Stream, Subject: original.Subject, NUID: w.spec.Identity.ObjectVersion, Index: index, Sequence: sequence,
		Message: infrarepair.MessageDigest(original.Header, original.Data)}, nil
}

func (w *recoveryWriterJS) remember(row infrarepair.ACK) error {
	w.ledger.mu.Lock()
	defer w.ledger.mu.Unlock()
	for _, prior := range w.ledger.acks {
		if prior.Index == row.Index {
			if prior != row {
				return infrarepair.ErrReceipt
			}
			return nil // Definite duplicate ACK, same original obligation.
		}
	}
	w.ledger.acks = append(w.ledger.acks, row)
	delete(w.ledger.pending, row.Index)
	return nil
}

// Stream preserves S7's lookup and selector semantics, while every original
// read joins the same gate as publish. Management verification uses a separate
// unwrapped connection; it can never insert repaired data into this ledger.
func (w *recoveryWriterJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	stream, err := w.JetStream.Stream(ctx, name)
	if err != nil || name != w.spec.Stream {
		return stream, err
	}
	return &recoveryStream{Stream: stream, writer: w}, nil
}

type recoveryStream struct {
	jetstream.Stream
	writer *recoveryWriterJS
}

func (s *recoveryStream) GetMsg(ctx context.Context, sequence uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	permit, err := s.writer.gate.Enter(s.writer.name)
	if err != nil {
		return nil, err
	}
	defer permit.Finish(nil) // Read-only; an unknown publish remains fenced independently.
	raw, err := s.Stream.GetMsg(ctx, sequence, opts...)
	if err != nil {
		return nil, err
	}
	// Selector-based direct reads choose a raw sequence first; only the exact
	// stored message at that sequence may confirm the retained attempt digest.
	raw, err = infrarepair.ReadOriginalMessage(ctx, s.writer.JetStream, s.Stream, raw.Sequence)
	if err != nil {
		return nil, err
	}
	if raw.Subject != xferbucket.ChunkSubject(s.writer.spec) {
		return raw, nil
	}
	index, e := strconv.ParseUint(raw.Header.Get("Tether-Xfer-Index"), 10, 32)
	if e != nil {
		return nil, infrarepair.ErrReceipt
	}
	s.writer.ledger.mu.Lock()
	pending, ok := s.writer.ledger.pending[index]
	s.writer.ledger.mu.Unlock()
	if ok {
		if pending.Subject != raw.Subject || pending.Message != infrarepair.MessageDigest(raw.Header, raw.Data) {
			return nil, infrarepair.ErrReceipt
		}
		pending.Sequence = raw.Sequence
		if err = s.writer.remember(pending); err != nil {
			return nil, err
		}
	}
	return raw, nil
}
