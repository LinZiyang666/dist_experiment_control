package filetransfer

import (
	"io"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/nats-io/nats.go/jetstream"
)

// legacyOnce is reached only by the plain cap0 carrier after continuation
// dispatch. It exposes no checkpoint, resume or retry; a failed SDK operation
// never falls back from the capable worker into this adapter.
// origin: hot-update S7 plan §7 / T49 / M01
func legacyOnce(js jetstream.JetStream, bucket, key string, source io.Reader, deadline controlbudget.Deadline) (*jetstream.ObjectInfo, error) {
	return jsstream.PutLegacyOnce(js, bucket, key, source, deadline)
}
