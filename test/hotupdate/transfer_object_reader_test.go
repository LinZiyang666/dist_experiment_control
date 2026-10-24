//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go/jetstream"
)

// Observe publication before doing any raw census. SDK metadata can legitimately
// be retired as soon as the original receiver settles; serial polling behind
// a full stream scan misses that window, even when the writer format is correct.
// A plain subscription adds no retained JetStream consumer to the drain census.
// origin: S7 internal review FIX-SDKGET F1 / INTEG-O item 7
func startTransferSDKRead(w *transferRawWatch) {
	w.t.Helper()
	ctx, cancel := context.WithCancel(w.t.Context())
	w.sdkCancel, w.sdkDone = cancel, make(chan struct{})
	subject := "$O." + proto.XferBucketName(w.s.sid) + ".M." + base64.URLEncoding.EncodeToString([]byte(w.r.id))
	sub, err := w.s.nc.SubscribeSync(subject)
	transferMust(w.t, err)
	// The subscription is installed before the caller resumes the transfer.
	// The initial GetInfo below also covers publication just before this barrier.
	transferMust(w.t, w.s.nc.FlushTimeout(time.Second))
	go func() {
		defer close(w.sdkDone)
		defer cancel()
		defer func() { w.recordSDKRead(false, sub.Unsubscribe()) }()
		store, err := w.s.js.ObjectStore(ctx, proto.XferBucketName(w.s.sid))
		if err != nil {
			w.recordSDKRead(false, err)
			return
		}
		info, err := store.GetInfo(ctx, w.r.id)
		if err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
			w.recordSDKRead(false, err)
			return
		}
		if err == nil {
			w.recordSDKRead(true, readTransferSDKObject(ctx, store, w, info))
			return
		}
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			if ctx.Err() == nil {
				w.recordSDKRead(false, err)
			}
			return
		}
		info = new(jetstream.ObjectInfo)
		if err = json.Unmarshal(msg.Data, info); err != nil {
			w.recordSDKRead(false, err)
			return
		}
		w.recordSDKRead(true, readTransferSDKObject(ctx, store, w, info))
	}()
}

func (w *transferRawWatch) recordSDKRead(read bool, err error) {
	w.sdkMu.Lock()
	defer w.sdkMu.Unlock()
	w.sdkErr = errors.Join(w.sdkErr, err)
	if read && err == nil {
		w.sdk = true
	}
}

func readTransferSDKObject(parent context.Context, store jetstream.ObjectStore, w *transferRawWatch, published *jetstream.ObjectInfo) error {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	want := sha256.Sum256(w.r.body)
	valid := func(info *jetstream.ObjectInfo) bool {
		return info != nil && !info.Deleted && info.Name == w.r.id &&
			info.Bucket == proto.XferBucketName(w.s.sid) && info.NUID == w.r.boundary.ObjectVersion &&
			info.Size == uint64(len(w.r.body)) && info.Digest == "SHA-256="+base64.URLEncoding.EncodeToString(want[:])
	}
	if !valid(published) {
		return fmt.Errorf("SDK Get disagrees with original object identity/content: published metadata=%+v", published)
	}
	reader, err := store.Get(ctx, w.r.id)
	if err != nil {
		return fmt.Errorf("SDK Get disagrees with original object identity/content: original version=%s: %w", published.NUID, err)
	}
	info, infoErr := reader.Info()
	hash := sha256.New()
	n, readErr := io.Copy(hash, reader)
	closeErr := reader.Close()
	if err = errors.Join(infoErr, readErr, closeErr); err != nil || !valid(info) ||
		n != int64(len(w.r.body)) || hex.EncodeToString(hash.Sum(nil)) != hex.EncodeToString(want[:]) {
		return fmt.Errorf("SDK Get disagrees with original object identity/content: bytes=%d metadata=%+v error=%v", n, info, err)
	}
	return nil
}

// startObjectReads attaches the independent SDK Get and raw chunk readers once
// the cut is complete and before the parked producer can publish further.
// The stream census already spans the cut; an SDK request in flight across a
// same-store restart is lost to the restart, not to the product.
// origin: S7 FIX-CUT live-cut barrier / S7 internal review FIX-SDKGET F1 / F2
func (w *transferRawWatch) startObjectReads() {
	if w.r.cell.tier != "b" || w.sdkDone != nil {
		return
	}
	startTransferSDKRead(w)
	startTransferRawChunks(w)
}

// awaitPublishedObjectReads keeps the producer parked while an already
// published original object is read independently. After release, its
// legitimate retirement would otherwise race the SDK Get and raw census.
// origin: S7 FIX-CUT live-cut barrier / S7 internal review FIX-SDKGET F1 / F2
func (w *transferRawWatch) awaitPublishedObjectReads() error {
	if w.r.cell.tier != "b" || w.sdkDone == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(w.t.Context(), 5*time.Second)
	defer cancel()
	store, err := w.s.js.ObjectStore(ctx, proto.XferBucketName(w.s.sid))
	if err != nil {
		return err
	}
	if _, err := store.GetInfo(ctx, w.r.id); errors.Is(err, jetstream.ErrObjectNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	limit := time.NewTimer(15 * time.Second)
	defer limit.Stop()
	for _, done := range []<-chan struct{}{w.sdkDone, w.chunks.done} {
		select {
		case <-done:
		case <-limit.C:
			return errors.New("independent reads of the published original did not finish while its producer was held")
		}
	}
	return nil
}
