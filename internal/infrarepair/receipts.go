package infrarepair

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ACK is captured from the original server acknowledgment, never synthesized
// from post-restart StreamInfo.LastSeq. NUID and Index bind the original S7
// obligation even when several writers share a backing stream.
type ACK struct {
	// TransferID is the original invocation, distinct from the object NUID.
	// Empty names a non-transfer receipt (audit or metadata), never a chunk.
	TransferID string
	Account    string
	Stream     string
	Subject    string
	NUID       string
	Index      uint64
	Sequence   uint64
	Message    c.Digest
}

type ConsumerReceipt struct {
	Account           string
	Stream            string
	Name              string
	CreatedNS         int64
	DeliveredConsumer uint64
	DeliveredStream   uint64
	ACKConsumer       uint64
	ACKStream         uint64
}

// LocalReceipt is the frozen owner state, not an instruction to reconstruct it.
// Identity binds the invocation/OFD lineage; Digest binds the reader's synced
// prefix or pending attempt. PublisherCursor preserves its pre-replay floor.
type LocalReceipt struct {
	Identity        c.Digest
	SyncedPrefix    uint64
	PrefixDigest    c.Digest
	PublisherCursor uint64
	PendingAttempt  c.Digest
}

type Responsibilities struct {
	ACKs      []ACK
	Consumers []ConsumerReceipt
	Local     []LocalReceipt
}

// MessageDigest uses an unambiguous encoding of both headers and payload. JSON
// orders map keys while preserving repeated header value order and byte content.
// No raw message contents leave the management read path in receipts or errors.
func MessageDigest(header nats.Header, payload []byte) c.Digest {
	// JetStream omits empty data in raw responses; nil and empty carry the same bytes.
	if len(payload) == 0 {
		payload = nil
	}
	if len(header) == 0 {
		header = nil
	}
	return digest(struct {
		Header  nats.Header
		Payload []byte
	}{header, payload})
}

func digest(value interface{}) c.Digest {
	data, err := json.Marshal(value)
	if err != nil {
		panic("infrarepair: internal receipt is not encodable")
	}
	return c.Digest(sha256.Sum256(data))
}

func cloneResponsibilities(r Responsibilities) Responsibilities {
	r.ACKs = append([]ACK(nil), r.ACKs...)
	r.Consumers = append([]ConsumerReceipt(nil), r.Consumers...)
	r.Local = append([]LocalReceipt(nil), r.Local...)
	return r
}

func validateResponsibilities(r Responsibilities, accounts map[string]jetstream.JetStream) error {
	for _, a := range r.ACKs {
		if accounts[a.Account] == nil || a.Stream == "" || a.Subject == "" || a.Sequence == 0 || a.Message == (c.Digest{}) {
			return c.ErrIncomplete
		}
	}
	for _, a := range r.Consumers {
		if accounts[a.Account] == nil || a.Stream == "" || a.Name == "" || a.CreatedNS <= 0 || a.ACKConsumer > a.DeliveredConsumer || a.ACKStream > a.DeliveredStream {
			return c.ErrIncomplete
		}
	}
	for _, a := range r.Local {
		if a.Identity == (c.Digest{}) || (a.SyncedPrefix != 0 && a.PrefixDigest == (c.Digest{})) {
			return c.ErrIncomplete
		}
	}
	return nil
}

// requestContext owns each bounded management request. Its explicit stop watcher
// is always joined before the scope returns; no worker outlives verification.
func requestContext(deadline c.Deadline, stop c.Stop) (context.Context, func(), error) {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), remaining) // ctx-none: ctx-free recovery management request.
	if stop == nil {
		return ctx, cancel, nil
	}
	// An explicit stop watcher is joined, including the deadline/error path.
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, func() { cancel(); <-done }, nil
}

// origin: hot-update S9 plan §4.3 / T40 / M20
func verifyResponsibilities(accounts map[string]jetstream.JetStream, r Responsibilities, deadline c.Deadline, stop c.Stop) error {
	ctx, finish, err := requestContext(deadline, stop)
	if err != nil {
		return err
	}
	defer finish()
	for _, a := range r.ACKs {
		if err := admission(deadline, stop); err != nil {
			return err
		}
		stream, err := accounts[a.Account].Stream(ctx, a.Stream)
		if err != nil {
			return readError(err)
		}
		msg, err := storedMessage(ctx, accounts[a.Account], stream, a.Sequence)
		if err != nil {
			return readError(err)
		}
		if msg.Sequence != a.Sequence || msg.Subject != a.Subject || MessageDigest(msg.Header, msg.Data) != a.Message {
			return ErrReceipt
		}
	}
	for _, a := range r.Consumers {
		if err := admission(deadline, stop); err != nil {
			return err
		}
		consumer, err := accounts[a.Account].Consumer(ctx, a.Stream, a.Name)
		if err != nil {
			return readError(err)
		}
		info, err := consumer.Info(ctx)
		if err != nil {
			return readError(err)
		}
		if info.Created.UnixNano() != a.CreatedNS || info.Delivered.Consumer != a.DeliveredConsumer || info.Delivered.Stream != a.DeliveredStream || info.AckFloor.Consumer != a.ACKConsumer || info.AckFloor.Stream != a.ACKStream {
			return ErrReceipt
		}
	}
	return admission(deadline, stop)
}

func readError(err error) error {
	if errors.Is(err, jetstream.ErrMsgNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) || errors.Is(err, jetstream.ErrConsumerNotFound) {
		return ErrReceipt
	}
	return err // Unavailable/503 is unknown, not a fabricated lost or failed transfer.
}
