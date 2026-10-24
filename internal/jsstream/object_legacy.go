package jsstream

import (
	"context"
	"io"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/nats-io/nats.go/jetstream"
)

// PutLegacyOnce is deliberately one primitive, without retries, rewind, or a
// continuation interface. The cap0 caller owns the invocation's one-shot gate.
// origin: hot-update S7 plan §7 / T01
func PutLegacyOnce(js jetstream.JetStream, bucket, name string, source io.Reader, deadline controlbudget.Deadline) (*jetstream.ObjectInfo, error) {
	ctx, cancel, err := writerContext(deadline)
	if err != nil {
		return nil, err
	}
	defer cancel()
	// The one-shot legacy owner may stop a stalled upload before its business
	// deadline. Cancellation joins the same Put; it never grants another Put.
	// origin: TestPutWithJSWatchdogCancelsAStalledPutOnlyAfterStrikes / hot-update S7 plan §7
	if owner, ok := source.(interface{ LegacyPutContext() context.Context }); ok {
		stop := context.AfterFunc(owner.LegacyPutContext(), cancel)
		defer stop()
	}
	store, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return store.Put(ctx, jetstream.ObjectMeta{Name: name}, source)
}
