package jsstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

var _ ReaderFactory = NewTransferReader

const transferReaderPullTimeout = time.Second

// Consumer expiry must cover the frozen 2108s byte budget and 120s recovery
// slack; successful Stop still deletes promptly instead of relying on expiry.
const transferReaderConsumerLifetime = (2108+120)*time.Second + time.Minute

// NewTransferReader installs a sink prefix without touching the object or sink.
// Network validation belongs to ReadChunk's absolute deadline. The receiver
// retains the original sink OFD and supplies only complete, truly synced chunks.
// origin: hot-update S7 plan §3 / §8.1 / T06
func NewTransferReader(js jetstream.JetStream, spec xfercontract.ObjectSpec, checkpoint xfercontract.SinkCheckpoint) (ResumeReader, error) {
	if js == nil || js.Conn() == nil {
		return nil, errors.New("transfer object: missing connection")
	}
	digest, err := hex.DecodeString(spec.SHA256)
	intent, intentErr := hex.DecodeString(spec.Identity.IntentDigest)
	created, timeErr := time.Parse(time.RFC3339Nano, spec.StreamCreated)
	if err != nil || len(digest) != sha256.Size || spec.SHA256 != strings.ToLower(spec.SHA256) ||
		intentErr != nil || len(intent) != sha256.Size || spec.Identity.IntentDigest != strings.ToLower(spec.Identity.IntentDigest) || timeErr != nil || created.IsZero() ||
		!validTransferBucket(spec.Bucket) || spec.Stream != "OBJ_"+spec.Bucket ||
		!objectChunkToken(spec.Identity.ObjectVersion) || !objectChunkToken(spec.Identity.TransferID) ||
		spec.Size < 0 || spec.Size > 2<<30 || spec.ChunkSize < 32<<10 || spec.ChunkSize > 1<<20 ||
		uint64(spec.Chunks) != (uint64(spec.Size)+uint64(spec.ChunkSize)-1)/uint64(spec.ChunkSize) || spec.Chunks > 65536 {
		return nil, errors.New("transfer object: invalid fixed object specification")
	}
	h, checkpoint, err := restoreSinkCheckpoint(spec, checkpoint)
	if err != nil {
		return nil, err
	}
	w, err := retainObjectWindow(js)
	if err != nil {
		return nil, err
	}
	// ctx-root: Stop or Freeze cancels and joins this reader's active network operation.
	root, cancel := context.WithCancel(context.Background())
	return &transferResumeReader{root: root, cancel: cancel, serial: make(chan struct{}, 1),
		js: js, spec: spec, created: created, digest: digest, checkpoint: checkpoint,
		hash: h, window: w, lastSeq: checkpoint.LastConsumedStreamSeq}, nil
}

// The same bucket-token contract already guards writer admission and cleanup.
// origin: S7 internal review TRANSFER-FOLLOWUPS F1 / hot-update S7 plan §2.1
func validTransferBucket(bucket string) bool { return xferbucket.ObjectBucketToken(bucket) }

func cloneSinkCheckpoint(cp xfercontract.SinkCheckpoint) xfercontract.SinkCheckpoint {
	cp.SHA256State = bytes.Clone(cp.SHA256State)
	return cp
}

func restoreSinkCheckpoint(spec xfercontract.ObjectSpec, cp xfercontract.SinkCheckpoint) (hash.Hash, xfercontract.SinkCheckpoint, error) {
	expectedOffset := min(int64(cp.ChunkIndex)*int64(spec.ChunkSize), spec.Size)
	if cp.ChunkIndex > spec.Chunks || cp.DurableOffset != expectedOffset ||
		(cp.ChunkIndex == 0) != (cp.LastConsumedStreamSeq == 0) || cp.LastConsumedStreamSeq == math.MaxUint64 {
		return nil, cp, errors.New("transfer object: invalid synced checkpoint cursor")
	}
	h := sha256.New()
	if len(cp.SHA256State) != 0 {
		if err := unmarshalSinkHash(h, cp.SHA256State); err != nil ||
			binary.BigEndian.Uint64(cp.SHA256State[len(cp.SHA256State)-8:]) != uint64(cp.DurableOffset) {
			return nil, cp, errors.New("transfer object: invalid synced checkpoint hash")
		}
	} else if cp.ChunkIndex != 0 {
		return nil, cp, errors.New("transfer object: missing synced checkpoint hash")
	}
	if cp.ChunkIndex == 0 && !bytes.Equal(h.Sum(nil), sha256.New().Sum(nil)) {
		return nil, cp, errors.New("transfer object: nonempty hash for empty synced prefix")
	}
	state, err := marshalSinkHash(h)
	if err != nil {
		return nil, cp, err
	}
	cp.SHA256State = state
	return h, cloneSinkCheckpoint(cp), nil
}

// The checkpoint's hash component is SHA-256's own marshaled state. Both
// directions are checked conversions: a hash that cannot round-trip its state
// refuses the cursor instead of panicking inside the receiver's reader owner.
func marshalSinkHash(h hash.Hash) ([]byte, error) {
	marshaler, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		return nil, errors.New("transfer object: synced checkpoint hash is not serializable")
	}
	return marshaler.MarshalBinary()
}

func unmarshalSinkHash(h hash.Hash, state []byte) error {
	unmarshaler, ok := h.(encoding.BinaryUnmarshaler)
	if !ok {
		return errors.New("transfer object: synced checkpoint hash is not restorable")
	}
	return unmarshaler.UnmarshalBinary(state)
}

type transferResumeReader struct {
	root         context.Context
	cancel       context.CancelFunc
	serial       chan struct{} // cancellation precedes bounded acquisition by Freeze/Stop
	mu           sync.Mutex    // checkpoint/pending/phase, never held across network IO
	js           jetstream.JetStream
	spec         xfercontract.ObjectSpec
	created      time.Time
	digest       []byte
	checkpoint   xfercontract.SinkCheckpoint
	hash         hash.Hash
	pending      *xfercontract.Chunk
	window       *objectWindow
	consumer     string
	sub          *nats.Subscription
	batch        []*nats.Msg
	ordinal      uint64 // consumer-local, reset to zero on every replacement
	lastSeq      uint64
	held         bool
	frozen       bool
	stopped      bool
	fatal        error
	activeCancel context.CancelFunc
}

func readerDeadlineContext(parent context.Context, deadline controlbudget.Deadline) (context.Context, context.CancelFunc, error) {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(parent, remaining)
	return ctx, cancel, nil
}

func (r *transferResumeReader) ReadChunk(deadline controlbudget.Deadline) (xfercontract.Chunk, error) {
	ctx, cancel, err := readerDeadlineContext(r.root, deadline)
	if err != nil {
		return xfercontract.Chunk{}, err
	}
	defer cancel()
	select {
	case r.serial <- struct{}{}:
		defer func() { <-r.serial }()
	case <-ctx.Done():
		return xfercontract.Chunk{}, ctx.Err()
	}
	r.mu.Lock()
	if r.frozen || r.stopped {
		r.mu.Unlock()
		return xfercontract.Chunk{}, errors.New("transfer object: reader is stopped or frozen")
	}
	r.activeCancel = cancel
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.activeCancel = nil
		r.mu.Unlock()
	}()
	for {
		if err := ctx.Err(); err != nil {
			return xfercontract.Chunk{}, err
		}
		r.mu.Lock()
		pending, cp, fatal := r.pending, r.checkpoint, r.fatal
		r.mu.Unlock()
		if fatal != nil {
			return xfercontract.Chunk{}, fatal
		}
		// A source seal authorizes streaming its prefix; only exact final
		// metadata authorizes exposing the last chunk to a committing sink.
		// Keep the pending chunk on a missing reply so retry cannot skip it.
		// The same gate covers an empty object's EOF, which has no last chunk.
		// origin: S7 internal review TRANSFER-FOLLOWUPS F1 / hot-update S7 plan §3 / T39
		if pending != nil && pending.Receipt.Index+1 == r.spec.Chunks || cp.ChunkIndex == r.spec.Chunks {
			if err := r.bounded(ctx, func(c context.Context) error {
				_, err := r.checkMetadata(c, true)
				return err
			}); err != nil {
				return xfercontract.Chunk{}, err
			}
		}
		if pending != nil {
			chunk := *pending
			chunk.Data = bytes.Clone(chunk.Data)
			return chunk, nil // retries cannot advance an unsynced sink
		}
		if r.sub == nil {
			if err = r.bounded(ctx, r.openConsumer); err != nil {
				return xfercontract.Chunk{}, err
			}
		}
		if cp.ChunkIndex == r.spec.Chunks {
			r.mu.Lock()
			complete := r.checkpoint.DurableOffset == r.spec.Size && bytes.Equal(r.hash.Sum(nil), r.digest)
			r.mu.Unlock()
			if !complete {
				return xfercontract.Chunk{}, r.reject(errors.New("transfer object: size or digest mismatch"))
			}
			r.releaseWindow()
			return xfercontract.Chunk{}, io.EOF
		}
		if len(r.batch) == 0 {
			r.releaseWindow()
			select {
			case r.window.gate <- struct{}{}:
				r.held = true
			case <-ctx.Done():
				return xfercontract.Chunk{}, ctx.Err()
			}
			if err = r.bounded(ctx, func(c context.Context) error { return drainObjectWindow(c, r.js.Conn(), r.window) }); err != nil {
				return xfercontract.Chunk{}, err
			}
			wanted := int(min(uint32(4), r.spec.Chunks-cp.ChunkIndex))
			attempt, stop := context.WithTimeout(ctx, transferReaderPullTimeout)
			limit := min(5*r.js.Conn().MaxPayload(), int64(32<<20))
			r.batch, err = r.sub.Fetch(wanted, nats.Context(attempt), nats.PullMaxBytes(int(limit)))
			stop() // Fetch is synchronous: it has joined before any UNSUB/PONG.
			dropped, dropErr := r.sub.Dropped()
			// A healthy Conn is not evidence of intact subscription delivery. Short
			// nil batches and a dropped last chunk must rebuild from the synced prefix.
			// origin: hot-update S7 plan §3 / T06 / M04
			if err != nil || dropErr != nil || dropped != 0 || len(r.batch) != wanted {
				if err = r.bounded(ctx, r.recoverConsumer); err != nil {
					return xfercontract.Chunk{}, err
				}
				continue
			}
		}
		msg := r.batch[0]
		r.batch = r.batch[1:]
		meta, metaErr := msg.Metadata()
		dropped, dropErr := r.sub.Dropped()
		// NumPending counts chunks published so far, not the sealed final size.
		// The exact ordinal, next index/offset and content headers still reject
		// missing/reordered chunks while a live sender grows this subject.
		// origin: S7 internal review TRANSFER-FOLLOWUPS F1 / hot-update S7 plan §3 / T39
		if metaErr != nil || dropErr != nil || dropped != 0 ||
			meta.Sequence.Consumer != r.ordinal+1 || meta.Sequence.Stream <= r.lastSeq ||
			meta.NumPending > uint64(r.spec.Chunks-cp.ChunkIndex-1) {
			if err = r.bounded(ctx, r.recoverConsumer); err != nil {
				return xfercontract.Chunk{}, err
			}
			continue
		}
		length := min(int64(r.spec.ChunkSize), r.spec.Size-cp.DurableOffset)
		if int64(len(msg.Data)) != length {
			return xfercontract.Chunk{}, r.reject(errors.New("transfer object: chunk violates fixed geometry"))
		}
		if err := validateTransferChunkHeaders(msg.Header, msg.Data, r.spec, cp.ChunkIndex); err != nil {
			return xfercontract.Chunk{}, r.reject(err)
		}
		r.ordinal++
		r.lastSeq = meta.Sequence.Stream
		chunk := xfercontract.Chunk{Receipt: xfercontract.ChunkReceipt{Index: cp.ChunkIndex,
			StreamSeq: r.lastSeq, Offset: cp.DurableOffset, Length: uint32(len(msg.Data)), SHA256: sha256.Sum256(msg.Data)},
			Data: bytes.Clone(msg.Data)}
		r.mu.Lock()
		r.pending = &chunk
		r.mu.Unlock()
		// Reenter through the pending branch, including its final metadata
		// gate. A streamed tail may precede the sender's Finalize ACK.
		// origin: S7 internal review TRANSFER-FOLLOWUPS F1 / hot-update S7 plan §3 / T39
		continue
	}
}

// The SDK format tolerates extension headers, but capable receivers must bind
// every chunk to its sealed intent and geometry. Duplicate values cannot be
// normalized away, and decimal/hex representations are canonical on the wire.
// origin: hot-update S7 plan §2.2 / §3 / T06
func validateTransferChunkHeaders(header nats.Header, data []byte, spec xfercontract.ObjectSpec, index uint32) error {
	receipt := xfercontract.ChunkReceipt{Index: index, Offset: int64(index) * int64(spec.ChunkSize), Length: uint32(len(data)), SHA256: sha256.Sum256(data)}
	return xferbucket.CheckChunkHeaders(spec, receipt, header)
}

func (r *transferResumeReader) reject(err error) error {
	r.mu.Lock()
	r.fatal = err
	r.mu.Unlock()
	return err
}

// Each recovery exchange has its own request-latency bound. One JetStream API
// reply lost across a same-store restart previously held the receiver, and
// its worker's entry token, for the rest of the byte budget. A bounded miss is
// transient owner recovery that the runner retries from the synced prefix; a
// rejected object and the caller's own expiry keep their original errors.
// origin: S7 FIX-CUT reader recovery stall / hot-update S7 plan §3 / T06 / M04
func (r *transferResumeReader) bounded(ctx context.Context, exchange func(context.Context) error) error {
	attempt, stop := context.WithTimeout(ctx, upgradebudget.RequestLatency)
	defer stop()
	err := exchange(attempt)
	r.mu.Lock()
	fatal := r.fatal
	r.mu.Unlock()
	if err == nil || fatal != nil || ctx.Err() != nil {
		return err
	}
	return fmt.Errorf("%s: transfer object reader recovery: %w", xfercontract.CodeOwnerRecovering, err)
}

// ConfirmSynced validates all four checkpoint components under one lock. It
// cannot observe Sync itself: the receiver calls this only after actual Sync,
// and no fetched bytes are hashed until this exact confirmation is accepted.
// origin: hot-update S7 plan §3 / T06
func (r *transferResumeReader) ConfirmSynced(cp xfercontract.SinkCheckpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen || r.stopped {
		return errors.New("transfer object: reader is stopped or frozen")
	}
	if cp.ChunkIndex == r.checkpoint.ChunkIndex && cp.DurableOffset == r.checkpoint.DurableOffset &&
		cp.LastConsumedStreamSeq == r.checkpoint.LastConsumedStreamSeq && bytes.Equal(cp.SHA256State, r.checkpoint.SHA256State) {
		return nil
	}
	if r.pending == nil {
		return errors.New("transfer object: confirmation has no fetched chunk")
	}
	receipt := r.pending.Receipt
	h := sha256.New()
	// The retained checkpoint was produced by marshalSinkHash, so a restore
	// failure is a corrupted owner: refuse the confirmation rather than hash
	// on top of an empty state that would silently disagree with the sink.
	if err := unmarshalSinkHash(h, r.checkpoint.SHA256State); err != nil {
		return fmt.Errorf("transfer object: retained checkpoint hash: %w", err)
	}
	_, _ = h.Write(r.pending.Data)
	state, err := marshalSinkHash(h)
	if err != nil {
		return err
	}
	if cp.ChunkIndex != receipt.Index+1 || cp.DurableOffset != receipt.Offset+int64(receipt.Length) ||
		cp.LastConsumedStreamSeq != receipt.StreamSeq || !bytes.Equal(cp.SHA256State, state) {
		return errors.New("transfer object: confirmation differs from fetched chunk")
	}
	r.hash, r.checkpoint, r.pending = h, cloneSinkCheckpoint(cp), nil
	return nil
}

func (r *transferResumeReader) openConsumer(ctx context.Context) error {
	// Resolve the old named delete before allocating another consumer. A
	// failed retirement cannot accumulate invisible server owners on retries.
	if err := r.retireConsumer(ctx); err != nil {
		return err
	}
	// Metadata is published after all source chunks. Requiring it here
	// prevents a pull receiver from ever syncing a live sender's prefix.
	// Existing metadata must still match; only authoritative absence may wait.
	// origin: S7 internal review TRANSFER-FOLLOWUPS F1 / hot-update S7 plan §3 / T39
	stream, err := r.checkMetadata(ctx, false)
	if err != nil {
		return err
	}
	r.mu.Lock()
	cp := r.checkpoint
	r.mu.Unlock()
	if cp.ChunkIndex != 0 {
		// Validate the saved sequence against the fixed subject, not merely its
		// numeric range. A forged cursor must not skip to another object's row.
		// origin: hot-update S7 plan §3 / T06
		msg, err := stream.GetMsg(ctx, cp.LastConsumedStreamSeq)
		length := min(int64(r.spec.ChunkSize), r.spec.Size-int64(cp.ChunkIndex-1)*int64(r.spec.ChunkSize))
		if err != nil {
			return err
		}
		if msg.Subject != "$O."+r.spec.Bucket+".C."+r.spec.Identity.ObjectVersion || int64(len(msg.Data)) != length {
			return r.reject(errors.New("transfer_object_version_mismatch: synced sequence belongs to another chunk"))
		}
		if err := validateTransferChunkHeaders(msg.Header, msg.Data, r.spec, cp.ChunkIndex-1); err != nil {
			return r.reject(err)
		}
	}
	if cp.ChunkIndex == r.spec.Chunks {
		return nil
	}
	r.consumer = nuid.Next() // recorded before send, including a sent-unknown create reply
	subject := "$O." + r.spec.Bucket + ".C." + r.spec.Identity.ObjectVersion
	config := jetstream.ConsumerConfig{Name: r.consumer, FilterSubject: subject, AckPolicy: jetstream.AckNonePolicy,
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: cp.LastConsumedStreamSeq + 1,
		InactiveThreshold: transferReaderConsumerLifetime, MaxRequestBatch: 4}
	_, err = r.js.CreateConsumer(ctx, r.spec.Stream, config)
	if err == nil {
		var legacy nats.JetStreamContext
		legacy, err = r.js.Conn().JetStream()
		if err == nil {
			// SkipConsumerLookup makes Bind purely local. Its context owns the
			// subscription lifetime; an individual ReadChunk timeout must not
			// unsubscribe it after returning one chunk from a four-message batch.
			r.sub, err = legacy.PullSubscribe(subject, "", nats.Bind(r.spec.Stream, r.consumer),
				nats.SkipConsumerLookup(), nats.AckNone(), nats.Context(r.root))
		}
	}
	if err != nil {
		_ = r.retireConsumer(ctx)
		return err
	}
	r.ordinal, r.lastSeq = 0, cp.LastConsumedStreamSeq
	return nil
}

func (r *transferResumeReader) releaseWindow() {
	if r.held {
		<-r.window.gate
		r.held = false
	}
}

func (r *transferResumeReader) retireConsumer(ctx context.Context) error {
	if r.sub != nil {
		_ = r.sub.Unsubscribe()
		r.sub = nil
	}
	var barrierErr error
	if r.held {
		setObjectWindowDirty(r.window, true)
		barrierErr = drainObjectWindow(ctx, r.js.Conn(), r.window)
		r.releaseWindow()
	}
	r.batch = nil
	if r.consumer != "" {
		cleanup := ConsumerDelete{Stream: r.spec.Stream, StreamCreated: r.created.UTC().Format(time.RFC3339Nano), Consumer: r.consumer}
		err := deleteObjectConsumer(ctx, r.js, cleanup)
		if err != nil {
			deferObjectConsumer(r.js.Conn(), r.window, cleanup)
			return errors.Join(barrierErr, fmt.Errorf("transfer object: delete consumer: %w", err))
		}
		r.consumer = ""
	}
	return barrierErr
}

func (r *transferResumeReader) recoverConsumer(ctx context.Context) error {
	if err := r.retireConsumer(ctx); err != nil {
		return err
	}
	// Repeated malformed/incomplete storage must consume the same deadline,
	// rather than spin or acquire a new transfer budget on every rebuild.
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *transferResumeReader) Freeze(deadline controlbudget.Deadline) (xfercontract.ReaderState, error) {
	ctx, cancel, err := readerDeadlineContext(context.WithoutCancel(r.root), deadline)
	if err != nil {
		return xfercontract.ReaderState{}, err
	}
	defer cancel()
	r.mu.Lock()
	r.frozen = true
	if r.activeCancel != nil {
		r.activeCancel()
	}
	r.mu.Unlock()
	select {
	case r.serial <- struct{}{}:
		defer func() { <-r.serial }()
	case <-ctx.Done():
		r.mu.Lock()
		r.frozen = false // a failed join preserves the original reader's admission
		r.mu.Unlock()
		return xfercontract.ReaderState{}, ctx.Err()
	}
	r.mu.Lock()
	r.frozen = true
	r.pending = nil // only the original receiver may repair its unsynced suffix
	cp := cloneSinkCheckpoint(r.checkpoint)
	r.mu.Unlock()
	consumer := r.consumer
	err = r.retireConsumer(ctx)
	if err != nil {
		r.mu.Lock()
		r.frozen = false
		r.mu.Unlock()
	}
	phase := xfercontract.ReaderOpen
	dirty := objectWindowDirty(r.window)
	if dirty {
		phase = xfercontract.ReaderDirty
	} else if r.consumer != "" {
		phase = xfercontract.ReaderDeletePending
	}
	return xfercontract.ReaderState{ObjectSpec: r.spec, Phase: phase, SinkCheckpoint: cp,
		ConsumerIdentity: consumer, ConnectionIdentity: r.window.identity, DirtyBarrier: dirty, DeletePending: r.consumer != ""}, err
}

func (r *transferResumeReader) Stop(deadline controlbudget.Deadline) error {
	ctx, cancel, err := readerDeadlineContext(context.WithoutCancel(r.root), deadline)
	if err != nil {
		return err
	}
	defer cancel()
	r.cancel()
	select {
	case r.serial <- struct{}{}:
		defer func() { <-r.serial }()
	case <-ctx.Done():
		return ctx.Err()
	}
	r.mu.Lock()
	already := r.stopped
	r.stopped = true
	r.pending = nil
	r.mu.Unlock()
	if already {
		// Stop has handed uncertain deletion to the independent lifecycle owner.
		// Retrying it through this dead data binding could resurrect a duty after
		// the current management binding already confirmed and released it.
		// origin: S7 internal review rv-jsnats F3
		objectWindows.Lock()
		pending := r.window.needsBarrier || len(r.window.deletes) != 0
		objectWindows.Unlock()
		if pending {
			return errors.New("transfer object: consumer cleanup pending")
		}
		return nil
	}
	err = r.retireConsumer(ctx)
	r.consumer = "" // any failed delete is now retained by the cleanup owner
	releaseObjectWindow(r.js.Conn(), r.window)
	return err
}

// checkMetadata distinguishes a still-publishing source from a foreign final
// object. The stream incarnation is rechecked even for an already fetched tail.
// origin: S7 internal review TRANSFER-FOLLOWUPS F1 / hot-update S7 plan §2.2 / §3 / T39
func (r *transferResumeReader) checkMetadata(ctx context.Context, required bool) (jetstream.Stream, error) {
	stream, err := r.js.Stream(ctx, r.spec.Stream)
	if err != nil {
		return nil, err
	}
	if !stream.CachedInfo().Created.Equal(r.created) {
		return nil, r.reject(errors.New("transfer_object_version_mismatch: stream incarnation changed"))
	}
	store, err := r.js.ObjectStore(ctx, r.spec.Bucket)
	if err != nil {
		return nil, err
	}
	info, err := store.GetInfo(ctx, r.spec.Identity.TransferID)
	if errors.Is(err, jetstream.ErrObjectNotFound) && !required {
		return stream, nil
	}
	if err != nil {
		return nil, err
	}
	digest, err := jetstream.DecodeObjectDigest(info.Digest)
	if err != nil || !strings.HasPrefix(info.Digest, "SHA-256=") || !bytes.Equal(digest, r.digest) ||
		info.Bucket != r.spec.Bucket || info.Name != r.spec.Identity.TransferID || info.Deleted ||
		info.NUID != r.spec.Identity.ObjectVersion || info.Size != uint64(r.spec.Size) || info.Chunks != r.spec.Chunks ||
		info.Opts == nil || info.Opts.Link != nil || info.Opts.ChunkSize != r.spec.ChunkSize || int64(info.Opts.ChunkSize) > r.js.Conn().MaxPayload() {
		return nil, r.reject(errors.New("transfer_object_version_mismatch: metadata differs from fixed object"))
	}
	return stream, nil
}
