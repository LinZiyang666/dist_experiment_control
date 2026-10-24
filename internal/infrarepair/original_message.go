package infrarepair

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// storedMessage reads the original persisted headers. The modern SDK's direct
// GetMsg leaves Nats-Stream/Sequence/Subject/Time-Stamp delivery headers in the
// result; hashing them would falsely reject every real S7 object-store ACK.
// Do not strip arbitrary headers from MessageDigest: that would hide a change
// to an original header. The SDK's non-direct API returns the stored message
// and preserves the configured account/domain/API prefix and request deadline.
// origin: hot-update S9 plan §4.3 / T40; NC-EXPORTS original ACK handoff
func storedMessage(ctx context.Context, js jetstream.JetStream, stream jetstream.Stream, seq uint64) (*jetstream.RawStreamMsg, error) {
	if !stream.CachedInfo().Config.AllowDirect {
		return stream.GetMsg(ctx, seq)
	}
	options := js.Options()
	var opts []nats.JSOpt
	if options.Domain != "" {
		opts = append(opts, nats.Domain(options.Domain))
	} else if options.APIPrefix != "" {
		opts = append(opts, nats.APIPrefix(options.APIPrefix))
	}
	legacy, err := js.Conn().JetStream(opts...)
	if err != nil {
		return nil, err
	}
	raw, err := legacy.GetMsg(stream.CachedInfo().Config.Name, seq, nats.Context(ctx))
	if errors.Is(err, nats.ErrStreamNotFound) {
		return nil, jetstream.ErrStreamNotFound
	}
	if errors.Is(err, nats.ErrMsgNotFound) {
		return nil, jetstream.ErrMsgNotFound
	}
	if err != nil {
		return nil, err
	}
	return &jetstream.RawStreamMsg{Subject: raw.Subject, Sequence: raw.Sequence, Header: raw.Header, Data: raw.Data, Time: raw.Time}, nil
}

// ReadOriginalMessage is the exact stored-message read for S9's registered S7
// transport. The caller owns its producer permit; this read cannot clear a
// sent-unknown transport fence or turn a post-repair lookup into an ACK.
func ReadOriginalMessage(ctx context.Context, js jetstream.JetStream, stream jetstream.Stream, seq uint64) (*jetstream.RawStreamMsg, error) {
	return storedMessage(ctx, js, stream, seq)
}
