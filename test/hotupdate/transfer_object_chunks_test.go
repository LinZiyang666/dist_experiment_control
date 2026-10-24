//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/test/transferharness"
	"github.com/nats-io/nats.go"
)

type transferRawChunkRead struct {
	mu       sync.Mutex
	rows     map[uint64]transferharness.RawMessage
	size     int64
	err      error
	sub      *nats.Subscription
	done     chan struct{}
	finished sync.Once
}

// A complete raw census can lag legitimate purge just like SDK polling. Retain
// actual chunk deliveries continuously, with the physical sequence supplied by
// JetStream, and merge them with the independently read raw stream rows. The
// observer retires its own consumer after the complete original byte count.
// origin: S7 internal review FIX-SDKGET F2 / INTEG-O item 7
func startTransferRawChunks(w *transferRawWatch) {
	w.t.Helper()
	r := &transferRawChunkRead{rows: make(map[uint64]transferharness.RawMessage), done: make(chan struct{})}
	w.chunks = r
	js, err := w.s.nc.JetStream()
	transferMust(w.t, err)
	subject := "$O." + proto.XferBucketName(w.s.sid) + ".C." + w.r.boundary.ObjectVersion
	closed := func(string) { r.finished.Do(func() { close(r.done) }) }
	r.sub, err = js.Subscribe(subject, func(msg *nats.Msg) {
		// The first delivery may precede Subscribe's return, including a one-
		// chunk object's immediate retirement. Install the join before closing.
		msg.Sub.SetClosedHandler(closed)
		meta, err := msg.Metadata()
		r.mu.Lock()
		if err == nil && (meta.Stream != "OBJ_"+proto.XferBucketName(w.s.sid) || meta.Sequence.Stream == 0 || msg.Subject != subject) {
			err = errors.New("raw object delivery has foreign stream or sequence")
		}
		if err == nil {
			headers := make(map[string][]string, len(msg.Header))
			for key, values := range msg.Header {
				headers[key] = append([]string(nil), values...)
			}
			row := transferharness.RawMessage{Sequence: meta.Sequence.Stream, Subject: msg.Subject, Data: bytes.Clone(msg.Data), Headers: headers}
			if old, ok := r.rows[row.Sequence]; ok {
				if old.Subject != row.Subject || !bytes.Equal(old.Data, row.Data) {
					err = errors.New("physical JetStream sequence changed content")
				}
			} else if len(r.rows) >= proto.TransferMaxChunks || r.size+int64(len(row.Data)) > int64(len(w.r.body)) {
				err = errors.New("raw object delivery exceeds original geometry")
			} else {
				r.rows[row.Sequence] = row
				r.size += int64(len(row.Data))
			}
		}
		r.err = errors.Join(r.err, err)
		complete := r.err != nil || r.size == int64(len(w.r.body))
		r.mu.Unlock()
		if complete {
			if err := msg.Sub.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrBadSubscription) {
				r.mu.Lock()
				r.err = errors.Join(r.err, err)
				r.mu.Unlock()
			}
		}
	}, nats.OrderedConsumer(), nats.BindStream("OBJ_"+proto.XferBucketName(w.s.sid)), nats.Context(w.t.Context()))
	transferMust(w.t, err)
	r.sub.SetClosedHandler(closed)
}

func (r *transferRawChunkRead) stop() {
	err := r.sub.Unsubscribe()
	if errors.Is(err, nats.ErrBadSubscription) {
		err = nil // The complete-byte callback already retired this consumer.
	}
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		err = errors.Join(err, errors.New("raw object delivery callback did not join"))
	}
	r.mu.Lock()
	r.err = errors.Join(r.err, err)
	r.mu.Unlock()
}

// The caller holds the raw census lock. Delivery has its own lock so a slow
// history scan cannot block capture of the last original chunk before purge.
func mergeTransferRawChunks(w *transferRawWatch) {
	if w.chunks == nil {
		return
	}
	r := w.chunks
	r.mu.Lock()
	defer r.mu.Unlock()
	w.err = errors.Join(w.err, r.err)
	name := "OBJ_" + proto.XferBucketName(w.s.sid)
	if w.rows[name] == nil {
		w.rows[name] = make(map[uint64]transferharness.RawMessage)
	}
	for seq, row := range r.rows {
		if old, ok := w.rows[name][seq]; ok && (old.Subject != row.Subject || !bytes.Equal(old.Data, row.Data)) {
			w.err = errors.Join(w.err, fmt.Errorf("physical JetStream sequence %d changed content", seq))
		}
		w.rows[name][seq] = row
	}
}
