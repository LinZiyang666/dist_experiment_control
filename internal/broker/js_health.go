package broker

import (
	"context"
	"errors"
	"fmt"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/nats-io/nats.go/jetstream"
)

// js_health.go — G7b #20③: classify the JetStream "system temporarily unavailable" (503) error so a
// SUSTAINED outage can be turned into a cluster alert instead of an every-~0.5s WARN that rots silently
// for days (the racknerd incident). Detection only — the sustained-window + synthetic-signal wiring
// lives in alert_reconcile.go; the surfacing rides ClusterHealthResp.JetStreamUnavailable (Option C).

// jsErrCodeUnavailable is the JetStream API error code for "JetStream system temporarily unavailable" —
// the 503 a clustered-JS meta with no quorum returns on every API call. nats.go/jetstream does not
// export a named const for it, so we pin the stable wire value (jetstream.APIError.ErrorCode).
const jsErrCodeUnavailable = 10008

// classifyJSUnavailable reports whether err is the JS-503 signal: a JetStream API error carrying code
// 10008, anywhere in the wrap chain. It matches ONLY that code — a stream-not-found, a request timeout,
// a context cancel, or a plain ErrNoResponders are NOT "JS is wedged at 503" and must not raise the
// alert (those are transient / structural and would flap or false-alarm).
func classifyJSUnavailable(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode == jsErrCodeUnavailable
	}
	return false
}

// classifyJSErrClass buckets an error into the COARSE class strings the h1 E3
// backoff trackers key their log discipline on. Classes must be enum-ish and
// stable — NEVER err.Error() (free text defeats suppression on any variable
// detail: a byte count, a stream name) — and coarse on purpose: two different
// api codes are two classes (each gets one logged line), but every occurrence
// of the same code is one class (suppressed after the first).
func classifyJSErrClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, jsstream.ErrMetaGroupNotReady):
		return "meta_not_ready"
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return "ctx"
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode == jsErrCodeUnavailable {
			return "js_unavailable"
		}
		return fmt.Sprintf("api:%d", apiErr.ErrorCode)
	}
	return "other"
}
