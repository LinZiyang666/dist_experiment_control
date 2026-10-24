package jsstream

// object_reader_test.go exercises the real JetStream object format and consumer lifecycle.
// The server, metadata, chunks, and pull requests are real; no object-store mock is used.
// WSS framing and bandwidth shaping are covered by the separate transport integration test.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func transferObjectFixture(t *testing.T, js jetstream.JetStream, bucket string) (context.Context, jetstream.ObjectStore, jetstream.Stream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	store, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket, Storage: jetstream.MemoryStorage})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, store, stream
}

func putTransferObject(ctx context.Context, t *testing.T, store jetstream.ObjectStore, body []byte) *jetstream.ObjectInfo {
	t.Helper()
	info, err := store.Put(ctx, jetstream.ObjectMeta{Name: "payload"}, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func replaceTransferObjectMetadata(ctx context.Context, t *testing.T, js jetstream.JetStream, bucket string, info *jetstream.ObjectInfo) {
	t.Helper()
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	msg := nats.NewMsg("$O." + bucket + ".M." + base64.URLEncoding.EncodeToString([]byte("payload")))
	msg.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
	msg.Data = data
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		t.Fatal(err)
	}
}

func requireNoTransferObjectConsumers(ctx context.Context, t *testing.T, stream jetstream.Stream) {
	t.Helper()
	var last uint64
	if !testharness.WaitFor(t, 3*time.Second, 10*time.Millisecond, func() bool {
		info, err := stream.Info(ctx)
		if err != nil {
			return false
		}
		last = uint64(info.State.Consumers)
		return last == 0
	}) {
		t.Fatalf("transfer reader left %d consumer(s) after Close", last)
	}
}

// origin: control-connection-recovery plan.
func TestTransferObjectReadsLargeAndEmptyObjects(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{name: "large", body: append(bytes.Repeat([]byte("object-reader-payload\x00\xff\x01"), 400000), 'x')},
		{name: "empty", body: []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, store, stream := transferObjectFixture(t, js, tc.name)
			putTransferObject(ctx, t, store, tc.body)
			r, err := GetTransferObject(ctx, js, store, tc.name, "payload")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Close() }()
			if len(tc.body) == 0 {
				info, err := stream.Info(ctx)
				if err != nil || info.State.Consumers != 0 {
					t.Fatalf("empty object allocated a consumer: info=%+v err=%v", info, err)
				}
			}
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tc.body) {
				t.Fatalf("object bytes changed: got=%d want=%d", len(got), len(tc.body))
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			requireNoTransferObjectConsumers(ctx, t, stream)
		})
	}
}

// origin: control-connection-recovery plan.
func TestTransferObjectStopsFetchingWhileReaderIsIdle(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx, store, stream := transferObjectFixture(t, js, "bounded")
	body := bytes.Repeat([]byte("0123456789abcdef"), 256*1024)
	putTransferObject(ctx, t, store, body)
	r, err := GetTransferObject(ctx, js, store, "bounded", "payload")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	one := []byte{0}
	if _, err := io.ReadFull(r, one); err != nil {
		t.Fatal(err)
	}
	var consumerName string
	for name := range stream.ConsumerNames(ctx).Name() {
		if consumerName != "" {
			t.Fatal("one reader created multiple simultaneous consumers")
		}
		consumerName = name
	}
	if consumerName == "" {
		t.Fatal("reader used no observable real consumer")
	}
	consumerInfo := func() *jetstream.ConsumerInfo {
		lister := stream.ListConsumers(ctx)
		for info := range lister.Info() {
			if info.Name == consumerName {
				return info
			}
		}
		return nil
	}
	var delivered uint64
	if !testharness.WaitFor(t, 5*time.Second, 5*time.Millisecond, func() bool {
		info := consumerInfo()
		if info == nil {
			return false
		}
		delivered = info.Delivered.Consumer
		return delivered >= 4
	}) {
		t.Fatalf("first bounded fetch never completed: delivered=%d", delivered)
	}
	// Round trips establish that the control connection and server keep processing while the
	// application has consumed only one byte. No next batch may be requested on its behalf.
	for range 10 {
		if err := js.Conn().FlushTimeout(2 * time.Second); err != nil {
			t.Fatal(err)
		}
		info := consumerInfo()
		if info == nil {
			t.Fatal("active object consumer disappeared")
		}
		if info.Delivered.Consumer > 4 {
			t.Fatalf("idle reader allowed %d chunks to be sent; want at most one four-chunk window", info.Delivered.Consumer)
		}
	}
	tail, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(append(one, tail...), body) {
		t.Fatal("resuming a paused reader changed the object")
	}
}

// origin: control-connection-recovery plan.
func TestTransferObjectRejectsCorruptMetadata(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	cases := []struct {
		name   string
		change func(*jetstream.ObjectInfo)
	}{
		{"empty_nuid", func(info *jetstream.ObjectInfo) { info.NUID = "" }},
		{"wildcard_nuid", func(info *jetstream.ObjectInfo) { info.NUID = ">" }},
		{"multi_token_nuid", func(info *jetstream.ObjectInfo) { info.NUID = "other.generation" }},
		{"space_nuid", func(info *jetstream.ObjectInfo) { info.NUID = "bad token" }},
		{"deleted", func(info *jetstream.ObjectInfo) { info.Deleted = true }},
		{"link", func(info *jetstream.ObjectInfo) {
			info.Opts.Link = &jetstream.ObjectLink{Bucket: info.Bucket, Name: "other"}
		}},
		{"empty_digest", func(info *jetstream.ObjectInfo) { info.Digest = "" }},
		{"invalid_digest", func(info *jetstream.ObjectInfo) { info.Digest = "SHA-256=not-base64!" }},
		{"wrong_digest", func(info *jetstream.ObjectInfo) {
			info.Digest = "SHA-256=" + base64.URLEncoding.EncodeToString(make([]byte, 32))
		}},
		{"short_declared_size", func(info *jetstream.ObjectInfo) { info.Size-- }},
		{"long_declared_size", func(info *jetstream.ObjectInfo) { info.Size++ }},
		{"zero_size_with_chunks", func(info *jetstream.ObjectInfo) { info.Size = 0 }},
		{"zero_chunks_with_size", func(info *jetstream.ObjectInfo) { info.Chunks = 0 }},
		{"too_few_chunks", func(info *jetstream.ObjectInfo) { info.Chunks-- }},
		{"too_many_chunks", func(info *jetstream.ObjectInfo) { info.Chunks++ }},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bucket := fmt.Sprintf("invalid-%d", i)
			ctx, store, stream := transferObjectFixture(t, js, bucket)
			body := bytes.Repeat([]byte("transfer-payload"), 24000)
			info := putTransferObject(ctx, t, store, body)
			if info.Chunks < 2 {
				t.Fatal("fixture must span multiple chunks")
			}
			tc.change(info)
			replaceTransferObjectMetadata(ctx, t, js, bucket, info)
			readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			r, err := GetTransferObject(readCtx, js, store, bucket, "payload")
			if err == nil {
				_, err = io.Copy(io.Discard, r)
				if closeErr := r.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}
			if err == nil {
				t.Fatal("corrupt object metadata was accepted as a complete object")
			}
			requireNoTransferObjectConsumers(ctx, t, stream)
		})
	}
}

// origin: control-connection-recovery plan.
func TestTransferObjectRejectsMissingAndAlteredChunks(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	for _, mutation := range []string{"missing_middle", "reordered_chunks", "extra_tail"} {
		t.Run(mutation, func(t *testing.T) {
			ctx, store, stream := transferObjectFixture(t, js, mutation)
			body := append(bytes.Repeat([]byte("a"), 128*1024), bytes.Repeat([]byte("b"), 128*1024)...)
			body = append(body, bytes.Repeat([]byte("c"), 128*1024)...)
			info := putTransferObject(ctx, t, store, body)
			middle, err := stream.GetMsg(ctx, 2)
			if err != nil || middle.Subject != "$O."+mutation+".C."+info.NUID {
				t.Fatalf("second stream message is not the expected middle chunk: msg=%+v err=%v", middle, err)
			}
			if mutation != "extra_tail" {
				if err := stream.DeleteMsg(ctx, 2); err != nil {
					t.Fatal(err)
				}
			}
			if mutation == "extra_tail" {
				if _, err := js.Publish(ctx, middle.Subject, []byte("unexpected tail")); err != nil {
					t.Fatal(err)
				}
			}
			if mutation == "reordered_chunks" {
				if _, err := js.Publish(ctx, middle.Subject, middle.Data); err != nil {
					t.Fatal(err)
				}
			}
			readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			r, err := GetTransferObject(readCtx, js, store, mutation, "payload")
			if err == nil {
				_, err = io.Copy(io.Discard, r)
				_ = r.Close()
			}
			if err == nil {
				t.Fatal("missing, reordered, or excess chunk was accepted as a complete object")
			}
			requireNoTransferObjectConsumers(ctx, t, stream)
		})
	}
}

// origin: control-connection-recovery plan.
func TestTransferObjectCloseAndCancelReleaseResources(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx, store, stream := transferObjectFixture(t, js, "cleanup")
	putTransferObject(ctx, t, store, bytes.Repeat([]byte("resource-check"), 150000))
	// Warm the shared request subscription before taking the baseline; the subject under test
	// owns consumers and fetch subscriptions, while the fixture owns its one NATS connection.
	warm, err := GetTransferObject(ctx, js, store, "cleanup", "payload")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, warm); err != nil {
		t.Fatal(err)
	}
	if err := warm.Close(); err != nil {
		t.Fatal(err)
	}
	requireNoTransferObjectConsumers(ctx, t, stream)
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	subscriptions := js.Conn().NumSubscriptions()
	for round := 0; round < 6; round++ {
		readCtx, cancel := context.WithCancel(ctx)
		r, err := GetTransferObject(readCtx, js, store, "cleanup", "payload")
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		one := []byte{0}
		if _, err := io.ReadFull(r, one); err != nil {
			cancel()
			_ = r.Close()
			t.Fatal(err)
		}
		if round%2 == 0 {
			cancel()
			if _, err := io.Copy(io.Discard, r); err == nil {
				t.Error("canceled partial read reported successful completion")
			}
		}
		if err := r.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
		cancel()
		if err := r.Close(); err != nil {
			t.Errorf("second Close: %v", err)
		}
		requireNoTransferObjectConsumers(ctx, t, stream)
	}
	if !testharness.WaitFor(t, 3*time.Second, 10*time.Millisecond, func() bool {
		return js.Conn().NumSubscriptions() <= subscriptions
	}) {
		t.Errorf("fetch subscriptions leaked: before=%d after=%d", subscriptions, js.Conn().NumSubscriptions())
	}
	testharness.AssertNoGoroutineLeak(t, "transfer object reader", goroutines)
	// No new connection belongs to this reader. Zero is the expected FD delta.
	testharness.AssertNoFDLeak(t, "transfer object reader", fds, 0)
}

// origin: control-connection-recovery plan.
func TestTransferObjectCanceledBeforeOpenCreatesNoConsumer(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx, store, stream := transferObjectFixture(t, js, "already-canceled")
	putTransferObject(ctx, t, store, []byte(strings.Repeat("data", 100)))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	r, err := GetTransferObject(canceled, js, store, "already-canceled", "payload")
	if r != nil {
		_ = r.Close()
	}
	if err == nil {
		t.Fatal("already canceled context opened a transfer reader")
	}
	requireNoTransferObjectConsumers(ctx, t, stream)
}

// origin: control-connection-recovery plan.
func TestTransferObjectCloseInterruptsBlockedRead(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	for _, stop := range []string{"close", "cancel"} {
		t.Run(stop, func(t *testing.T) {
			ctx, store, stream := transferObjectFixture(t, js, "blocked-"+stop)
			info := putTransferObject(ctx, t, store, []byte("one chunk"))
			first, err := stream.GetMsg(ctx, 1)
			if err != nil || first.Subject != "$O.blocked-"+stop+".C."+info.NUID {
				t.Fatalf("fixture first message is not its chunk: %v %v", first, err)
			}
			if err := stream.DeleteMsg(ctx, 1); err != nil {
				t.Fatal(err)
			}
			readCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			r, err := GetTransferObject(readCtx, js, store, "blocked-"+stop, "payload")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Close() }()
			readDone := make(chan error, 1)
			go func() {
				_, err := io.ReadAll(r)
				readDone <- err
			}()
			// A real waiting pull proves Read is blocked in the production fetch path;
			// observing a goroutine start or sleeping would not establish that premise.
			if !testharness.WaitFor(t, 5*time.Second, 10*time.Millisecond, func() bool {
				waiting := false
				for ci := range stream.ListConsumers(ctx).Info() {
					waiting = waiting || ci.NumWaiting > 0
				}
				return waiting
			}) {
				t.Fatal("reader never issued the expected blocking pull")
			}
			closeDone := make(chan error, 1)
			if stop == "cancel" {
				cancel()
			}
			go func() { closeDone <- r.Close() }()
			select {
			case err := <-readDone:
				if err == nil {
					t.Error("interrupted incomplete object returned successful EOF")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close/cancel failed to interrupt blocked Read")
			}
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not join the interrupted fetch")
			}
			requireNoTransferObjectConsumers(ctx, t, stream)
		})
	}
}

// origin: control-connection-recovery internal transport review.
func TestTransferObjectReadersShareOneConnectionWindow(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx, store, stream := transferObjectFixture(t, js, "shared-window")
	ctx, stopReaders := context.WithCancel(ctx)
	defer stopReaders()
	body := bytes.Repeat([]byte("shared-connection-payload"), 90000)
	putTransferObject(ctx, t, store, body)
	const readers = 20
	all := make([]io.ReadCloser, 0, readers)
	for range readers {
		r, err := GetTransferObject(ctx, js, store, "shared-window", "payload")
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, r)
		t.Cleanup(func() { _ = r.Close() })
	}
	start, resume := make(chan struct{}), make(chan struct{})
	entered, firstByte, done := make(chan struct{}, readers), make(chan struct{}, readers), make(chan error, readers)
	for _, r := range all {
		go func(r io.ReadCloser) {
			<-start
			entered <- struct{}{}
			one := []byte{0}
			if _, err := io.ReadFull(r, one); err != nil {
				done <- err
				return
			}
			firstByte <- struct{}{}
			select {
			case <-resume:
			case <-ctx.Done():
				done <- ctx.Err()
				return
			}
			tail, err := io.ReadAll(r)
			if err == nil && !bytes.Equal(append(one, tail...), body) {
				err = fmt.Errorf("concurrent reader changed object bytes")
			}
			if closeErr := r.Close(); err == nil {
				err = closeErr
			}
			done <- err
		}(r)
	}
	close(start)
	for range readers {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("concurrent readers did not start")
		}
	}
	select {
	case <-firstByte:
	case <-ctx.Done():
		t.Fatal("no concurrent reader made progress")
	}
	countDelivered := func() uint64 {
		var total uint64
		for ci := range stream.ListConsumers(ctx).Info() {
			total += ci.Delivered.Consumer
		}
		return total
	}
	if !testharness.WaitFor(t, 5*time.Second, 10*time.Millisecond, func() bool { return countDelivered() >= 4 }) {
		t.Error("no full batch reached a reader")
	}
	for range 10 {
		if err := js.Conn().FlushTimeout(2 * time.Second); err != nil {
			t.Error(err)
			break
		}
		if total := countDelivered(); total > 4 {
			t.Errorf("one connection delivered %d chunks to idle readers; per-reader windows accumulate beyond the shared four-chunk limit", total)
			break
		}
	}
	close(resume)
	for range readers {
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Fatal("resumed concurrent readers did not finish")
		}
	}
	requireNoTransferObjectConsumers(ctx, t, stream)
}

// origin: control-connection-recovery internal transport review.
func TestTransferObjectQueuedClosePreservesActiveWindow(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx, store, stream := transferObjectFixture(t, js, "queued-close")
	putTransferObject(ctx, t, store, bytes.Repeat([]byte("queued-reader"), 100000))
	open := func() io.ReadCloser {
		t.Helper()
		r, err := GetTransferObject(ctx, js, store, "queued-close", "payload")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		return r
	}
	holder := open()
	if _, err := io.ReadFull(holder, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	queued := open()
	queuedRead := make(chan error, 1)
	go func() {
		_, err := queued.Read(make([]byte, 1))
		queuedRead <- err
	}()
	closeDone := make(chan error, 1)
	go func() { closeDone <- queued.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close could not cancel a reader waiting for the connection window")
	}
	select {
	case err := <-queuedRead:
		if err == nil {
			t.Fatal("closed queued reader returned object data")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queued Read remained blocked after Close")
	}
	// Closing a waiter must not release the permit still owned by holder.
	third := open()
	thirdDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, third)
		thirdDone <- err
	}()
	for range 10 {
		if err := js.Conn().FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		var delivered uint64
		for ci := range stream.ListConsumers(ctx).Info() {
			delivered += ci.Delivered.Consumer
		}
		if delivered > 4 {
			t.Fatalf("closing queued reader released active reader's window: delivered=%d", delivered)
		}
	}
	if err := holder.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-thirdDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiting reader did not progress after active reader closed")
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	requireNoTransferObjectConsumers(ctx, t, stream)
}

// origin: control-connection-recovery internal transport review.
func TestTransferObjectConnectionsHaveIndependentWindows(t *testing.T) {
	url := startJSNATS(t)
	firstJS, secondJS := newJS(t, url), newJS(t, url)
	ctx, store, stream := transferObjectFixture(t, firstJS, "independent-windows")
	body := bytes.Repeat([]byte("independent-connection"), 100000)
	putTransferObject(ctx, t, store, body)
	first, err := GetTransferObject(ctx, firstJS, store, "independent-windows", "payload")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	if _, err := io.ReadFull(first, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	secondCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	secondStore, err := secondJS.ObjectStore(secondCtx, "independent-windows")
	if err != nil {
		t.Fatal(err)
	}
	second, err := GetTransferObject(secondCtx, secondJS, secondStore, "independent-windows", "payload")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	got, err := io.ReadAll(second)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("paused reader on another connection blocked or corrupted transfer: bytes=%d err=%v", len(got), err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	requireNoTransferObjectConsumers(ctx, t, stream)
}

// origin: control-connection-recovery internal transport review.
func TestTransferObjectConsumerLivesThroughTransferBudget(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx, store, stream := transferObjectFixture(t, js, "long-budget")
	putTransferObject(ctx, t, store, []byte("one chunk"))
	longCtx, cancel := context.WithTimeout(context.Background(), 11*time.Minute)
	defer cancel()
	r, err := GetTransferObject(longCtx, js, store, "long-budget", "payload")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var consumers int
	for ci := range stream.ListConsumers(ctx).Info() {
		consumers++
		if ci.Config.InactiveThreshold < 11*time.Minute {
			t.Fatalf("inactive consumer expires before an allowed transfer pause: threshold=%v", ci.Config.InactiveThreshold)
		}
	}
	if consumers != 1 {
		t.Fatalf("want one real consumer, got %d", consumers)
	}
}

// origin: control-connection-recovery internal transport review.
func TestTransferObjectRejectsHistoricalOversizedChunks(t *testing.T) {
	for _, metadata := range []string{"normal", "missing_options", "false_small_chunk_size"} {
		t.Run(metadata, func(t *testing.T) {
			opts := natstest.DefaultTestOptions
			opts.Port, opts.JetStream, opts.StoreDir = -1, true, t.TempDir()
			opts.MaxPayload = 1024 * 1024
			server := natstest.RunServer(&opts)
			t.Cleanup(func() { server.Shutdown(); server.WaitForShutdown() })
			writerJS := newJS(t, server.ClientURL())
			ctx, store, stream := transferObjectFixture(t, writerJS, "historical")
			const storedChunkSize = 256 * 1024
			body := bytes.Repeat([]byte("h"), 4*storedChunkSize+7)
			info, err := store.Put(ctx, jetstream.ObjectMeta{
				Name: "payload", Opts: &jetstream.ObjectMetaOptions{ChunkSize: storedChunkSize},
			}, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			switch metadata {
			case "missing_options":
				info.Opts = nil
			case "false_small_chunk_size":
				info.Opts.ChunkSize = 1024
			}
			replaceTransferObjectMetadata(ctx, t, writerJS, "historical", info)
			// Reload after the valid upload: max_payload limits new publications,
			// and does not shrink old chunk messages already stored in JetStream.
			lowered := opts
			lowered.MaxPayload = 32 * 1024
			if err := server.ReloadOptions(&lowered); err != nil {
				t.Fatal(err)
			}
			readerJS := newJS(t, server.ClientURL())
			if readerJS.Conn().MaxPayload() != int64(lowered.MaxPayload) {
				t.Fatalf("new receiver did not learn lowered max_payload: %d", readerJS.Conn().MaxPayload())
			}
			readerStore, err := readerJS.ObjectStore(ctx, "historical")
			if err != nil {
				t.Fatal(err)
			}
			readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			r, err := GetTransferObject(readCtx, readerJS, readerStore, "historical", "payload")
			if err == nil {
				_, err = io.Copy(io.Discard, r)
				_ = r.Close()
			}
			if err == nil {
				t.Fatal("actual old chunk exceeds the entire current WSS frame budget, but reader accepted it")
			}
			if err := readerJS.Conn().FlushTimeout(time.Second); err != nil {
				t.Fatalf("rejecting historical oversized data damaged control connection: %v", err)
			}
			requireNoTransferObjectConsumers(ctx, t, stream)
		})
	}
}

type transferReadPauseConn struct {
	net.Conn
	mu        sync.Mutex
	resume    chan struct{}
	blocked   chan struct{}
	sentPings chan struct{}
}

func (c *transferReadPauseConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.mu.Lock()
	resume, blocked := c.resume, c.blocked
	c.mu.Unlock()
	if resume != nil {
		select {
		case blocked <- struct{}{}:
		default:
		}
		<-resume
	}
	return n, err
}

func (c *transferReadPauseConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if bytes.Contains(p[:n], []byte("PING\r\n")) {
		select {
		case c.sentPings <- struct{}{}:
		default:
		}
	}
	return n, err
}

func (c *transferReadPauseConn) pauseReads() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resume = make(chan struct{})
	c.blocked = make(chan struct{}, 1)
	return c.blocked
}

func (c *transferReadPauseConn) resumeReads() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.resume != nil {
		close(c.resume)
		c.resume = nil
	}
}

func (c *transferReadPauseConn) Close() error {
	c.resumeReads()
	return c.Conn.Close()
}

type transferReadPauseDialer struct{ conn *transferReadPauseConn }

func (d *transferReadPauseDialer) Dial(network, address string) (net.Conn, error) {
	conn, err := net.DialTimeout(network, address, 2*time.Second)
	if err != nil {
		return nil, err
	}
	d.conn = &transferReadPauseConn{Conn: conn, sentPings: make(chan struct{}, 100)}
	return d.conn, nil
}

// origin: control-connection-recovery internal transport review.
func TestTransferObjectFailedBarrierBlocksNextReaderUntilRecovery(t *testing.T) {
	url := startJSNATS(t)
	observerJS := newJS(t, url)
	ctx, store, stream := transferObjectFixture(t, observerJS, "failed-barrier")
	body := bytes.Repeat([]byte("f"), 1024*1024)
	putTransferObject(ctx, t, store, body)
	dialer := &transferReadPauseDialer{}
	nc, err := nats.Connect(url, nats.SetCustomDialer(dialer), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	receiverStore, err := js.ObjectStore(ctx, "failed-barrier")
	if err != nil {
		t.Fatal(err)
	}
	open := func() io.ReadCloser {
		t.Helper()
		r, err := GetTransferObject(ctx, js, receiverStore, "failed-barrier", "payload")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		return r
	}
	first, second := open(), open()
	if _, err := io.ReadFull(first, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	blocked := dialer.conn.pauseReads()
	defer dialer.conn.resumeReads()
	closeDone := make(chan error, 1)
	go func() { closeDone <- first.Close() }()
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("fixture did not withhold the cleanup response from the NATS reader")
	}
	select {
	case <-closeDone:
		// Cleanup must return even though its bounded barrier and delete replies
		// are held below the protocol parser. The server remains alive.
	case <-time.After(6 * time.Second):
		t.Fatal("Close did not bound an unavailable cleanup response")
	}
	for len(dialer.conn.sentPings) > 0 {
		<-dialer.conn.sentPings
	}
	requests, err := observerJS.Conn().SubscribeSync("$JS.API.CONSUMER.MSG.NEXT.OBJ_failed-barrier.>")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = requests.Unsubscribe() }()
	if err := observerJS.Conn().FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		got, err := io.ReadAll(second)
		if err == nil && !bytes.Equal(got, body) {
			err = fmt.Errorf("recovered reader changed object bytes")
		}
		readDone <- err
	}()
	select {
	case <-dialer.conn.sentPings:
		// An actual outgoing protocol PING proves the second reader reached its
		// inherited barrier, rather than merely not being scheduled yet.
	case err := <-readDone:
		t.Fatalf("reader failed before the recoverable barrier was released: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("next reader did not attempt to resolve the inherited drain barrier")
	}
	if err := observerJS.Conn().FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if pending, _, err := requests.Pending(); err != nil || pending != 0 {
		t.Fatalf("new pull was published before the old barrier completed: pending=%d err=%v", pending, err)
	}
	dialer.conn.resumeReads()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reader failed to recover when the blocked network resumed")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	requireNoTransferObjectConsumers(ctx, t, stream)
}
