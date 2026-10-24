package jsstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

func transferReaderDeadline(t *testing.T, budget time.Duration) controlbudget.Deadline {
	t.Helper()
	deadline, err := controlbudget.After(budget)
	if err != nil {
		t.Fatal(err)
	}
	return deadline
}

func transferReaderSpec(ctx context.Context, t *testing.T, js jetstream.JetStream, store jetstream.ObjectStore, stream jetstream.Stream, body []byte) xfercontract.ObjectSpec {
	t.Helper()
	// Independent wire-format fixture: real JS publications and SDK metadata
	// readback. This is not an implementation of P02's receipt/CAS writer.
	streamInfo, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	intent := sha256.Sum256([]byte("reader fixture intent"))
	spec := xfercontract.ObjectSpec{Identity: xfercontract.Identity{TransferID: "payload", ObjectVersion: nuid.Next(), IntentDigest: hex.EncodeToString(intent[:])},
		Bucket: streamInfo.Config.Name[len("OBJ_"):], Stream: streamInfo.Config.Name, StreamCreated: streamInfo.Created.Format(time.RFC3339Nano),
		SHA256: hex.EncodeToString(digest[:]), Size: int64(len(body)), ChunkSize: 32 << 10, Chunks: uint32((len(body) + (32 << 10) - 1) / (32 << 10))}
	for index := uint32(0); index < spec.Chunks; index++ {
		offset := int(index * spec.ChunkSize)
		data := body[offset:min(offset+int(spec.ChunkSize), len(body))]
		msg := transferReaderFixtureChunk(spec, index, data)
		if _, err := js.PublishMsg(ctx, msg); err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			// Another object's physical sequence is a legal hole in this
			// subject. Delivery ordinals and stream sequences must stay distinct.
			if _, err := js.Publish(ctx, "$O."+spec.Bucket+".C."+nuid.Next(), []byte("other object")); err != nil {
				t.Fatal(err)
			}
		}
	}
	info := &jetstream.ObjectInfo{ObjectMeta: jetstream.ObjectMeta{Name: "payload", Opts: &jetstream.ObjectMetaOptions{ChunkSize: spec.ChunkSize}},
		Bucket: spec.Bucket, NUID: spec.Identity.ObjectVersion, Size: uint64(len(body)), Chunks: spec.Chunks,
		Digest: "SHA-256=" + base64.URLEncoding.EncodeToString(digest[:]), ModTime: time.Now().UTC()}
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	msg := nats.NewMsg("$O." + spec.Bucket + ".M." + base64.URLEncoding.EncodeToString([]byte("payload")))
	msg.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
	msg.Data = data
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		t.Fatal(err)
	}
	readback, err := store.GetInfo(ctx, "payload")
	if err != nil || readback.NUID != spec.Identity.ObjectVersion {
		t.Fatalf("SDK metadata readback: %+v %v", readback, err)
	}
	return spec
}

func transferReaderFixtureChunk(spec xfercontract.ObjectSpec, index uint32, data []byte) *nats.Msg {
	msg := nats.NewMsg("$O." + spec.Bucket + ".C." + spec.Identity.ObjectVersion)
	msg.Data = data
	digest := sha256.Sum256(data)
	msg.Header.Set("Tether-Xfer-Version", "1")
	msg.Header.Set("Tether-Xfer-Index", strconv.FormatUint(uint64(index), 10))
	msg.Header.Set("Tether-Xfer-Offset", strconv.FormatUint(uint64(index)*uint64(spec.ChunkSize), 10))
	msg.Header.Set("Tether-Xfer-Length", strconv.Itoa(len(data)))
	msg.Header.Set("Tether-Xfer-SHA256", hex.EncodeToString(digest[:]))
	msg.Header.Set("Tether-Xfer-Intent", spec.Identity.IntentDigest)
	return msg
}

// All operations still go to the embedded server. Record replacements so the
// oracle checks restart sequence/ordinal rather than just a final matching hash.
type observedTransferReaderJS struct {
	jetstream.JetStream
	mu      sync.Mutex
	configs []jetstream.ConsumerConfig
	rebound chan jetstream.ConsumerConfig
}

func (j *observedTransferReaderJS) CreateConsumer(ctx context.Context, stream string, config jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	consumer, err := j.JetStream.CreateConsumer(ctx, stream, config)
	if err == nil {
		j.mu.Lock()
		j.configs = append(j.configs, config)
		j.mu.Unlock()
		if j.rebound != nil {
			select {
			case j.rebound <- config:
			default: // recording must never block a product recovery loop
			}
		}
	}
	return consumer, err
}

func (j *observedTransferReaderJS) snapshot() []jetstream.ConsumerConfig {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]jetstream.ConsumerConfig(nil), j.configs...)
}

func syncTransferReaderChunk(t *testing.T, reader ResumeReader, sink *os.File, h hash.Hash, chunk xfercontract.Chunk) xfercontract.SinkCheckpoint {
	t.Helper()
	n, err := sink.WriteAt(chunk.Data, chunk.Receipt.Offset)
	if err != nil || n != len(chunk.Data) {
		t.Fatalf("actual sink WriteAt: n=%d err=%v", n, err)
	}
	if err := sink.Sync(); err != nil {
		t.Fatal(err)
	}
	_, _ = h.Write(chunk.Data)
	state, err := h.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	cp := xfercontract.SinkCheckpoint{ChunkIndex: chunk.Receipt.Index + 1, DurableOffset: chunk.Receipt.Offset + int64(n),
		LastConsumedStreamSeq: chunk.Receipt.StreamSeq, SHA256State: state}
	if err := reader.ConfirmSynced(cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

// origin: hot-update S7 plan §3 / T06 / M04
func TestTransferReaderRecoversHealthyConnectionDrops(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferReaderRecovery(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer reader recovery", goroutines)
	testharness.AssertNoFDLeak(t, "transfer reader recovery", fds, 0)
}

func exerciseTransferReaderRecovery(t *testing.T) {
	t.Helper()
	for _, failure := range []string{"slow_consumer", "last_chunk_drop", "consumer_deleted", "short_nil_batch", "delivery_gap"} {
		t.Run(failure, func(t *testing.T) { exerciseTransferReaderFault(t, failure) })
	}
}

func exerciseTransferReaderFault(t *testing.T, failure string) {
	t.Helper()
	url := startJSNATS(t)
	observer := newJS(t, url)
	ctx, store, stream := transferObjectFixture(t, observer, "recover")
	body := make([]byte, 8*(32<<10)+7)
	for i := range body {
		body[i] = byte(i*31 + i/(32<<10))
	}
	spec := transferReaderSpec(ctx, t, observer, store, stream, body)
	slowErrors := make(chan error, 32)
	dialer := &transferReadPauseDialer{}
	nc, err := nats.Connect(url, nats.SetCustomDialer(dialer), nats.NoReconnect(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		select {
		case slowErrors <- err:
		default:
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	actualJS, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	js := &observedTransferReaderJS{JetStream: actualJS, rebound: make(chan jetstream.ConsumerConfig, 32)}
	reader, err := NewTransferReader(js, spec, xfercontract.SinkCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Stop(transferReaderDeadline(t, 3*time.Second)) }()
	r := reader.(*transferResumeReader)
	sink, err := os.CreateTemp(t.TempDir(), "sink")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sink.Close() }()
	h := sha256.New()
	deadline := transferReaderDeadline(t, 12*time.Second)
	var cp xfercontract.SinkCheckpoint
	prefixChunks := 4
	if failure == "last_chunk_drop" {
		prefixChunks = int(spec.Chunks) - 1
	}
	for range prefixChunks {
		chunk, err := reader.ReadChunk(deadline)
		if err != nil {
			t.Fatal(err)
		}
		cp = syncTransferReaderChunk(t, reader, sink, h, chunk)
	}
	initial := js.snapshot()[0].Name
	recoveryStart := cp.LastConsumedStreamSeq + 1
	var restore func()
	switch failure {
	case "consumer_deleted":
		if err := observer.DeleteConsumer(ctx, spec.Stream, initial); err != nil {
			t.Fatal(err)
		}
	case "delivery_gap":
		// Consume a real batch behind the sink's back. The next live delivery
		// jumps consumer ordinal, though its healthy connection never reconnects.
		r.releaseWindow()
		batch, err := r.sub.Fetch(1, nats.Context(ctx))
		if err != nil || len(batch) != 1 {
			t.Fatalf("real skipped delivery: batch=%d err=%v", len(batch), err)
		}
	case "slow_consumer", "last_chunk_drop":
		// The socket is truly stalled below nats.go while the server sends the
		// next batch. A byte cap smaller than one real message forces an actual
		// ErrSlowConsumer, without relying on parser/goroutine scheduling.
		blocked := dialer.conn.pauseReads()
		defer dialer.conn.resumeReads()
		limit := int(spec.ChunkSize) / 2
		if failure == "last_chunk_drop" {
			limit = 1
		}
		if err := r.sub.SetPendingLimits(4, limit); err != nil {
			t.Fatal(err)
		}
		// Restoring reads after actual receive activity models recovery of the
		// same physical slow socket, not a Disconnect/Reconnect callback.
		restored := make(chan struct{})
		go func() {
			select {
			case <-blocked:
				dialer.conn.resumeReads()
			case <-ctx.Done():
			}
			close(restored)
		}()
		restore = func() { <-restored }
	case "short_nil_batch":
		// Hide only the final suffix, retaining the original object metadata.
		// Fetch returns its available prefix with nil after its bounded expiry.
		// Reinsert that real suffix only after a replacement consumer exists.
		var tail []*jetstream.RawStreamMsg
		for seq := uint64(spec.Chunks); seq <= uint64(spec.Chunks+1); seq++ {
			msg, err := stream.GetMsg(ctx, seq)
			if err != nil {
				t.Fatal(err)
			}
			tail = append(tail, msg)
			if err := stream.DeleteMsg(ctx, seq); err != nil {
				t.Fatal(err)
			}
		}
		for len(js.rebound) > 0 {
			<-js.rebound
		}
		published := make(chan error, 1)
		go func() {
			select {
			case config := <-js.rebound:
				if config.OptStartSeq != cp.LastConsumedStreamSeq+1 {
					published <- errors.New("short batch rebuilt past synced prefix")
					return
				}
				for _, msg := range tail {
					if _, err := observer.PublishMsg(ctx, &nats.Msg{Subject: msg.Subject, Header: msg.Header, Data: msg.Data}); err != nil {
						published <- err
						return
					}
				}
				published <- nil
			case <-ctx.Done():
				published <- ctx.Err()
			}
		}()
		restore = func() {
			if err := <-published; err != nil {
				t.Fatal(err)
			}
		}
	}
	for {
		progressStarted := time.Now()
		chunk, err := reader.ReadChunk(deadline)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if chunk.Receipt.Index != cp.ChunkIndex || chunk.Receipt.Offset != cp.DurableOffset {
			t.Fatalf("recovery skipped or repeated sink prefix: chunk=%+v checkpoint=%+v", chunk.Receipt, cp)
		}
		cp = syncTransferReaderChunk(t, reader, sink, h, chunk)
		if elapsed := time.Since(progressStarted); elapsed > upgradebudget.TransferProgress {
			t.Fatalf("reader exceeded the frozen durable progress window: %v", elapsed)
		}
	}
	if restore != nil {
		restore()
	}
	if failure == "slow_consumer" || failure == "last_chunk_drop" {
		select {
		case err := <-slowErrors:
			if !errors.Is(err, nats.ErrSlowConsumer) {
				t.Fatalf("fault did not produce actual ErrSlowConsumer: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("real slow-consumer premise missing")
		}
	}
	configs := js.snapshot()
	if len(configs) < 2 || configs[1].Name == initial || configs[1].OptStartSeq != recoveryStart {
		t.Fatalf("reader did not rebuild from the exact synced stream sequence: %+v", configs)
	}
	if !nc.IsConnected() || nc.Stats().Reconnects != 0 {
		t.Fatalf("healthy-connection recovery reconnected: %+v", nc.Stats())
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(sink.Name())
	if err != nil || !bytes.Equal(got, body) || cp.ChunkIndex != spec.Chunks || cp.DurableOffset != int64(len(body)) {
		t.Fatalf("synced sink changed after recovery: bytes=%d checkpoint=%+v err=%v", len(got), cp, err)
	}
	if err := reader.Stop(transferReaderDeadline(t, 3*time.Second)); err != nil {
		t.Fatal(err)
	}
	requireNoTransferObjectConsumers(ctx, t, stream)
}

// origin: hot-update S7 plan §3 / T06
func TestTransferReaderExportsOnlyConfirmedSinkPrefix(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferReaderCheckpoint(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer reader checkpoint", goroutines)
	testharness.AssertNoFDLeak(t, "transfer reader checkpoint", fds, 0)
}

func exerciseTransferReaderCheckpoint(t *testing.T) {
	t.Helper()
	t.Run("synced_prefix", func(t *testing.T) {
		js := newJS(t, startJSNATS(t))
		ctx, store, stream := transferObjectFixture(t, js, "prefix")
		body := bytes.Repeat([]byte("original prefix and suffix"), 10000)
		spec := transferReaderSpec(ctx, t, js, store, stream, body)
		reader, err := NewTransferReader(js, spec, xfercontract.SinkCheckpoint{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Stop(transferReaderDeadline(t, 3*time.Second)) }()
		deadline := transferReaderDeadline(t, 5*time.Second)
		chunk, err := reader.ReadChunk(deadline)
		if err != nil {
			t.Fatal(err)
		}
		original := bytes.Clone(chunk.Data)
		chunk.Data[0] ^= 255 // caller aliases must not poison the owner's pending hash
		replayed, err := reader.ReadChunk(deadline)
		if err != nil || !bytes.Equal(replayed.Data, original) || replayed.Receipt != chunk.Receipt {
			t.Fatal("unconfirmed ReadChunk advanced or exposed mutable owner bytes")
		}
		sink, err := os.CreateTemp(t.TempDir(), "sink")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sink.Close() }()
		h := sha256.New()
		cp := syncTransferReaderChunk(t, reader, sink, h, replayed)
		second, err := reader.ReadChunk(deadline)
		if err != nil {
			t.Fatal(err)
		}
		nextHash := sha256.New()
		_ = nextHash.(encoding.BinaryUnmarshaler).UnmarshalBinary(cp.SHA256State)
		_, _ = nextHash.Write(second.Data)
		nextState, _ := nextHash.(encoding.BinaryMarshaler).MarshalBinary()
		next := xfercontract.SinkCheckpoint{ChunkIndex: cp.ChunkIndex + 1, DurableOffset: cp.DurableOffset + int64(len(second.Data)), LastConsumedStreamSeq: second.Receipt.StreamSeq, SHA256State: nextState}
		for _, defect := range []string{"offset", "index", "sequence", "hash"} {
			bad := cloneSinkCheckpoint(next)
			switch defect {
			case "offset":
				bad.DurableOffset++
			case "index":
				bad.ChunkIndex++
			case "sequence":
				bad.LastConsumedStreamSeq++
			case "hash":
				bad.SHA256State[10] ^= 1
			}
			if err := reader.ConfirmSynced(bad); err == nil {
				t.Fatalf("confirmation accepted changed %s", defect)
			}
		}
		state, err := reader.Freeze(deadline)
		if err != nil || state.SinkCheckpoint.ChunkIndex != cp.ChunkIndex ||
			state.SinkCheckpoint.DurableOffset != cp.DurableOffset || !bytes.Equal(state.SinkCheckpoint.SHA256State, cp.SHA256State) {
			t.Fatalf("freeze included fetched unsynced suffix: state=%+v err=%v", state, err)
		}
		if err := reader.Stop(deadline); err != nil {
			t.Fatal(err)
		}
		resumed, err := NewTransferReader(js, spec, state.SinkCheckpoint)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resumed.Stop(transferReaderDeadline(t, 3*time.Second)) }()
		for {
			chunk, err := resumed.ReadChunk(deadline)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			cp = syncTransferReaderChunk(t, resumed, sink, h, chunk)
		}
		// EOF is only honest once the last confirmed cursor covers the object.
		if cp.ChunkIndex != spec.Chunks || cp.DurableOffset != spec.Size {
			t.Fatalf("resumed reader reached EOF at cursor %d/%d, want %d/%d", cp.ChunkIndex, cp.DurableOffset, spec.Chunks, spec.Size)
		}
		got, err := os.ReadFile(sink.Name())
		if err != nil || !bytes.Equal(got, body) {
			t.Fatal("resumed sink lost or duplicated its original prefix")
		}
		if err := resumed.Stop(deadline); err != nil {
			t.Fatal(err)
		}
		requireNoTransferObjectConsumers(ctx, t, stream)
	})
}

// origin: hot-update S7 plan §2.2 / §3 / T06
func TestTransferReaderRejectsChangedVersionAndInvalidCheckpoint(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferReaderValidation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer reader validation", goroutines)
	testharness.AssertNoFDLeak(t, "transfer reader validation", fds, 0)
}

func exerciseTransferReaderValidation(t *testing.T) {
	t.Helper()
	for _, defect := range []string{"version", "geometry", "incarnation", "digest", "checkpoint_sequence", "missing_header", "duplicate_header", "wrong_index", "wrong_offset", "wrong_length", "wrong_sha", "wrong_intent", "empty", "capacity"} {
		t.Run(defect, func(t *testing.T) { exerciseTransferReaderInvalidObject(t, defect) })
	}
}

func exerciseTransferReaderInvalidObject(t *testing.T, defect string) {
	t.Helper()
	js := newJS(t, startJSNATS(t))
	ctx, store, stream := transferObjectFixture(t, js, "invalid-reader")
	body := bytes.Repeat([]byte("sealed data and source"), 5000)
	if defect == "empty" {
		body = nil
	}
	spec := transferReaderSpec(ctx, t, js, store, stream, body)
	if defect == "capacity" {
		var readers []ResumeReader
		defer func() {
			for _, reader := range readers {
				_ = reader.Stop(transferReaderDeadline(t, time.Second))
			}
		}()
		for range objectWindowLimit {
			reader, err := NewTransferReader(js, spec, xfercontract.SinkCheckpoint{})
			if err != nil {
				t.Fatal(err)
			}
			readers = append(readers, reader)
		}
		reader, err := NewTransferReader(js, spec, xfercontract.SinkCheckpoint{})
		if reader != nil {
			readers = append(readers, reader)
		}
		if err == nil {
			t.Fatal("reader capacity overflow discarded cleanup reservation")
		}
		return
	}
	checkpoint := xfercontract.SinkCheckpoint{}
	switch defect {
	case "version", "geometry":
		info, err := store.GetInfo(ctx, "payload")
		if err != nil {
			t.Fatal(err)
		}
		if defect == "version" {
			info.NUID = nuid.Next()
		} else {
			info.Opts.ChunkSize++
		}
		replaceTransferObjectMetadata(ctx, t, js, spec.Bucket, info)
	case "incarnation":
		created, err := time.Parse(time.RFC3339Nano, spec.StreamCreated)
		if err != nil {
			t.Fatal(err)
		}
		spec.StreamCreated = created.Add(-time.Second).Format(time.RFC3339Nano)
	case "digest":
		// Both seal and metadata agree on the wrong full hash. Only actual
		// synced bytes at EOF can reject this structurally valid object.
		info, err := store.GetInfo(ctx, "payload")
		if err != nil {
			t.Fatal(err)
		}
		wrong := sha256.Sum256([]byte("different bytes"))
		spec.SHA256 = hex.EncodeToString(wrong[:])
		info.Digest = "SHA-256=" + base64.URLEncoding.EncodeToString(wrong[:])
		replaceTransferObjectMetadata(ctx, t, js, spec.Bucket, info)
	case "checkpoint_sequence":
		h := sha256.New()
		_, _ = h.Write(body[:spec.ChunkSize])
		state, _ := h.(encoding.BinaryMarshaler).MarshalBinary()
		checkpoint = xfercontract.SinkCheckpoint{ChunkIndex: 1, DurableOffset: int64(spec.ChunkSize), LastConsumedStreamSeq: uint64(spec.Chunks + 2), SHA256State: state}
	case "empty":
	default:
		// Alter the last chunk so raw stream holes remain valid while the
		// independent header predicate must reject the same sealed object.
		index := spec.Chunks - 1
		data := body[int(index*spec.ChunkSize):]
		msg := transferReaderFixtureChunk(spec, index, data)
		switch defect {
		case "missing_header":
			msg.Header.Del("Tether-Xfer-Version")
		case "duplicate_header":
			msg.Header.Add("Tether-Xfer-Intent", spec.Identity.IntentDigest)
		case "wrong_index":
			msg.Header.Set("Tether-Xfer-Index", "0")
		case "wrong_offset":
			msg.Header.Set("Tether-Xfer-Offset", "0")
		case "wrong_length":
			msg.Header.Set("Tether-Xfer-Length", "0")
		case "wrong_sha":
			msg.Header.Set("Tether-Xfer-SHA256", strings.Repeat("0", 64))
		case "wrong_intent":
			msg.Header.Set("Tether-Xfer-Intent", strings.Repeat("0", 64))
		default:
			t.Fatalf("unknown fixture defect %q", defect)
		}
		if err := stream.DeleteMsg(ctx, uint64(spec.Chunks+1)); err != nil {
			t.Fatal(err)
		}
		if _, err := js.PublishMsg(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := NewTransferReader(js, spec, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Stop(transferReaderDeadline(t, 3*time.Second)) }()
	sink, err := os.CreateTemp(t.TempDir(), "validation-sink")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sink.Close() }()
	h := sha256.New()
	var readErr error
	deadline := transferReaderDeadline(t, 3*time.Second)
	for {
		chunk, err := reader.ReadChunk(deadline)
		if err != nil {
			readErr = err
			break
		}
		_ = syncTransferReaderChunk(t, reader, sink, h, chunk)
	}
	if defect == "empty" {
		if !errors.Is(readErr, io.EOF) {
			t.Fatalf("empty sealed object: %v", readErr)
		}
	} else if errors.Is(readErr, io.EOF) || errors.Is(readErr, context.DeadlineExceeded) || readErr == nil {
		t.Fatalf("invalid %s did not fail closed at its validation guard: %v", defect, readErr)
	}
	if err := reader.Stop(transferReaderDeadline(t, 3*time.Second)); err != nil {
		t.Fatal(err)
	}
	requireNoTransferObjectConsumers(ctx, t, stream)
}

// origin: hot-update S7 plan §3 / T06
func TestTransferReaderRejectsUnsyncedCheckpointState(t *testing.T) {
	spec := xfercontract.ObjectSpec{Size: 3 * (32 << 10), ChunkSize: 32 << 10, Chunks: 3}
	h := sha256.New()
	_, _ = h.Write(bytes.Repeat([]byte("a"), 32<<10))
	state, _ := h.(encoding.BinaryMarshaler).MarshalBinary()
	valid := xfercontract.SinkCheckpoint{ChunkIndex: 1, DurableOffset: 32 << 10, LastConsumedStreamSeq: 7, SHA256State: state}
	for _, defect := range []string{"partial_offset", "beyond_end", "missing_sequence", "sequence_overflow", "missing_hash", "truncated_hash", "hash_ahead", "empty_nonempty_hash"} {
		t.Run(defect, func(t *testing.T) {
			cp := cloneSinkCheckpoint(valid)
			switch defect {
			case "partial_offset":
				cp.DurableOffset--
			case "beyond_end":
				cp.ChunkIndex = 4
				cp.DurableOffset = spec.Size
			case "missing_sequence":
				cp.LastConsumedStreamSeq = 0
			case "sequence_overflow":
				cp.LastConsumedStreamSeq = ^uint64(0)
			case "missing_hash":
				cp.SHA256State = nil
			case "truncated_hash":
				cp.SHA256State = cp.SHA256State[:4]
			case "hash_ahead":
				cp.SHA256State[len(cp.SHA256State)-1]++
			case "empty_nonempty_hash":
				cp.ChunkIndex, cp.DurableOffset, cp.LastConsumedStreamSeq = 0, 0, 0
				cp.SHA256State[10] ^= 1
				clear(cp.SHA256State[len(cp.SHA256State)-8:])
			}
			if _, _, err := restoreSinkCheckpoint(spec, cp); err == nil {
				t.Fatalf("checkpoint accepted %s", defect)
			}
		})
	}
	restored, copied, err := restoreSinkCheckpoint(spec, valid)
	if err != nil || !bytes.Equal(restored.Sum(nil), h.Sum(nil)) {
		t.Fatalf("valid complete prefix rejected: %v", err)
	}
	copied.SHA256State[10] ^= 1
	if bytes.Equal(copied.SHA256State, valid.SHA256State) {
		t.Fatal("imported hash state aliased its caller's checkpoint")
	}
}
