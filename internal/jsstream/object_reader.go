package jsstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// GetTransferObject reads a completed transfer object with demand-driven pulls.
// ObjectStore.Get uses an ordered push consumer whose flow-control window can
// reach 32 MiB. A slow WSS link lets the server combine that backlog into a frame
// larger than nats.go's 8*MaxPayload limit, disconnecting the control connection.
// Four messages, fully consumed before the next request, leave headroom for
// framing and control traffic without changing the standard object wire format.
// The caller must Close the reader, including after EOF or a read error.
func GetTransferObject(ctx context.Context, js jetstream.JetStream, store jetstream.ObjectStore, bucket, name string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Retain before ANY request: a creator racing an interrupted reader must
	// inherit that connection's drain barrier, even while binding its consumer.
	window, err := retainObjectWindow(js)
	if err != nil {
		return nil, err
	}
	owned := false
	defer func() {
		if !owned {
			releaseObjectWindow(js.Conn(), window)
		}
	}()
	info, err := store.GetInfo(ctx, name)
	if err != nil {
		return nil, err
	}
	if info.Bucket != bucket || info.Name != name || info.Deleted ||
		(info.Opts != nil && info.Opts.Link != nil) || !objectChunkToken(info.NUID) ||
		(info.Size == 0) != (info.Chunks == 0) || uint64(info.Chunks) > info.Size {
		return nil, errors.New("transfer object: invalid metadata")
	}
	digest, err := jetstream.DecodeObjectDigest(info.Digest)
	if err != nil || !strings.HasPrefix(info.Digest, "SHA-256=") || len(digest) != sha256.Size {
		return nil, errors.New("transfer object: invalid digest")
	}
	// A bucket can outlive a change of the server's max_payload setting. Do
	// not ask it to send old oversized chunks into the new connection's frame
	// limit. Normal ObjectStore.Put records the chosen chunk size in metadata.
	if info.Opts != nil && int64(info.Opts.ChunkSize) > js.Conn().MaxPayload() {
		return nil, errors.New("transfer object: stored chunk size exceeds current max_payload")
	}
	rctx, cancel := context.WithCancel(ctx)
	r := &transferObjectReader{ctx: rctx, cancel: cancel, js: js, info: info,
		stream: "OBJ_" + bucket, digest: digest, hash: sha256.New(), window: window}
	if info.Chunks != 0 {
		inactive := 5 * time.Minute
		if deadline, ok := ctx.Deadline(); ok {
			// A slow sink or another reader holding the connection window can
			// pause pulls. Do not expire a consumer before its transfer budget.
			inactive = max(inactive, time.Until(deadline)+time.Minute)
		}
		// Bind deletion to the incarnation observed before CREATE. A later stream
		// with the same name must never inherit this consumer's retirement duty.
		// origin: S7 internal review rv-jsnats F3
		stream, streamErr := js.Stream(ctx, r.stream)
		if streamErr != nil {
			cancel()
			return nil, streamErr
		}
		r.streamCreated = stream.CachedInfo().Created.UTC().Format(time.RFC3339Nano)
		r.consumerName = nuid.Next()
		r.consumer, err = js.CreateConsumer(ctx, r.stream, jetstream.ConsumerConfig{
			Name: r.consumerName, FilterSubject: "$O." + bucket + ".C." + info.NUID,
			AckPolicy: jetstream.AckNonePolicy, DeliverPolicy: jetstream.DeliverAllPolicy,
			// Close deletes promptly. This bounds server leftovers when the link
			// is unavailable, while allowing a slow filesystem to pause a reader.
			InactiveThreshold: inactive, MaxRequestBatch: 4,
		})
		if err != nil {
			// A lost create reply still leaves a named consumer cleanup responsibility.
			owned = true
			_ = r.Close()
			return nil, err
		}
	}
	owned = true // Close now owns the reference and server cleanup
	return r, nil
}

type transferObjectReader struct {
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.Mutex // serializes Read and cleanup; Close cancels before acquiring
	js            jetstream.JetStream
	stream        string
	info          *jetstream.ObjectInfo
	consumer      jetstream.Consumer
	consumerName  string
	streamCreated string
	window        *objectWindow
	held          bool
	sub           *nats.Subscription
	batch         []*nats.Msg
	batchErr      error
	buf           []byte
	chunks        uint32
	size          uint64
	seq           uint64
	digest        []byte
	hash          hash.Hash
	err           error
	closed        bool
	closeErr      error
}

func (r *transferObjectReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.buf) == 0 && r.err == nil {
		r.err = r.nextChunk()
	}
	if r.err != nil {
		return 0, r.err
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *transferObjectReader) nextChunk() error {
	for {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		if len(r.batch) > 0 {
			msg := r.batch[0]
			r.batch = r.batch[1:]
			meta, err := msg.Metadata()
			if err != nil || meta.Sequence.Consumer != uint64(r.chunks)+1 ||
				meta.Sequence.Stream <= r.seq || meta.NumPending != uint64(r.info.Chunks-r.chunks-1) {
				return errors.New("transfer object: chunk sequence gap")
			}
			data := msg.Data
			if len(data) == 0 || uint64(len(data)) > r.info.Size-r.size {
				return errors.New("transfer object: chunk exceeds declared size")
			}
			r.seq = meta.Sequence.Stream
			r.chunks++
			r.size += uint64(len(data))
			_, _ = r.hash.Write(data)
			r.buf = data
			return nil
		}
		if r.held {
			if r.batchErr != nil {
				// Keep the permit until Close's UNSUB/PONG barrier. An interrupted
				// fetch may still have data queued in the server or socket.
				return r.batchErr
			}
			r.releaseBatch() // a complete batch has physically arrived and been consumed
		}
		if r.chunks == r.info.Chunks {
			if r.size != r.info.Size || !bytes.Equal(r.hash.Sum(nil), r.digest) {
				return errors.New("transfer object: size or digest mismatch")
			}
			return io.EOF
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case r.window.gate <- struct{}{}:
			r.held = true
		}
		if err := drainObjectWindow(r.ctx, r.js.Conn(), r.window); err != nil {
			return err
		}
		wanted := int(min(uint32(4), r.info.Chunks-r.chunks))
		// nats.go caps WSS frames at min(8*MaxPayload, 64 MiB). Request a
		// strict wire-byte limit as well as a message count; metadata can lie,
		// and stored chunks can predate a server's max_payload reduction.
		limit := min(5*r.js.Conn().MaxPayload(), int64(32<<20))
		// The legacy public Fetch API supports simultaneous message AND byte
		// limits. Bind to the consumer we own; never create another consumer.
		// Bind normally performs an extra ConsumerInfo RPC that does not
		// inherit the caller context in this SDK. We just created and verified
		// this consumer; bind its known configuration without that lookup.
		// Each batch gets its own subscription: Fetch returns on the full count
		// before a trailing 409 Batch Completed necessarily arrives. Retiring
		// that inbox keeps its late status out of the next batch's wait loop,
		// without retrying a sent pull or suppressing a current error/short batch.
		// origin: S7 internal review OBJECT-READER-409 F1
		legacy, bindErr := r.js.Conn().JetStream()
		if bindErr == nil {
			r.sub, bindErr = legacy.PullSubscribe("$O."+r.info.Bucket+".C."+r.info.NUID, "",
				nats.Bind(r.stream, r.consumer.CachedInfo().Name), nats.SkipConsumerLookup(),
				nats.AckNone(), nats.Context(r.ctx))
		}
		if bindErr != nil {
			return bindErr
		}
		r.batch, r.batchErr = r.sub.Fetch(wanted, nats.Context(r.ctx), nats.PullMaxBytes(int(limit)))
		if len(r.batch) != wanted && r.batchErr == nil {
			r.batchErr = io.ErrUnexpectedEOF
		}
		if len(r.batch) == 0 {
			return r.batchErr
		}
	}
}

func (r *transferObjectReader) releaseBatch() {
	if r.held {
		_ = r.sub.Unsubscribe()
		r.sub = nil
		<-r.window.gate
		r.held = false
	}
}

func (r *transferObjectReader) Close() error {
	r.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	// Unsubscribe before the barrier. The server handles UNSUB and PING in
	// connection order; after its PONG is read no old subscription data can
	// remain queued for this socket, including late cluster-routed deliveries.
	if r.sub != nil {
		_ = r.sub.Unsubscribe()
	}
	if r.held {
		setObjectWindowDirty(r.window, true)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), objectCleanupTimeout)
		_ = drainObjectWindow(ctx, r.js.Conn(), r.window)
		cancel()
		// On failure the next reader must complete the barrier BEFORE Fetch.
		// Do not turn an uncertain drain into permission to queue another batch.
		r.releaseBatch()
	}
	r.batch = nil
	if r.consumerName != "" {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), objectCleanupTimeout)
		defer cancel()
		cleanup := ConsumerDelete{Stream: r.stream, StreamCreated: r.streamCreated, Consumer: r.consumerName}
		// This creation binding already checked the stream before CREATE. Send
		// its named DELETE promptly even when replies are blocked; another INFO
		// here would stall cancellation before any cleanup request is dispatched.
		// An absent consumer needs fresh incarnation proof, as do all retries
		// through the independent owner's replaceable management connection.
		// origin: S7 internal review rv-jsnats F3
		err := r.js.DeleteConsumer(ctx, cleanup.Stream, cleanup.Consumer)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			err = deleteObjectConsumer(ctx, r.js, cleanup)
		}
		if err != nil {
			deferObjectConsumer(r.js.Conn(), r.window, cleanup)
			r.closeErr = fmt.Errorf("transfer object: delete consumer: %w", err)
		}
	}
	releaseObjectWindow(r.js.Conn(), r.window)
	return r.closeErr
}
