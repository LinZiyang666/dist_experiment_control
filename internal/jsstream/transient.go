package jsstream

import (
	"context"
	"errors"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// JetStream API error codes this package classifies. nats.go v1.52.0 exports constants for only
// two of them (ErrorCode 10039 / 10076), so the rest are written out here with the identifier the
// server generates them from — nats-server v2.14.0 `server/jetstream_errors_generated.go`. This is
// therefore only HALF structural: the codes are stable API contract, but we spell them ourselves.
//
// Deliberately NOT a blanket `Code == 400` rule. 10005 (JSClusterNoPeersErrF) and 10202
// (JSClusterServerMemberChangeInflightErr, literally "cluster member change is in progress") are
// BOTH HTTP-class 400 and BOTH transient — and 10202 is the headline #67 window, a cluster grow in
// flight. A blanket 400 rule would make this classifier a no-op for the exact case it exists for.
const (
	jsErrClusterNotAvail      = jetstream.ErrorCode(10008) // JSClusterNotAvailErr — meta has no quorum yet
	jsErrClusterPeerNotMember = jetstream.ErrorCode(10040) // JSClusterPeerNotMemberErr — peer not in the group (yet)
	jsErrClusterNoPeers       = jetstream.ErrorCode(10005) // JSClusterNoPeersErrF — no suitable peer for placement
	jsErrClusterIncomplete    = jetstream.ErrorCode(10004) // JSClusterIncompleteErr — cluster still forming
	// EXTERNAL REVIEW F8: the G67 plan listed 10023 as transient but the implementation relied on the
	// English "insufficient" substring catching it. That works only while the server's wording holds —
	// a rewording, a localisation or a wrapper would silently reclassify it as PERMANENT and kill the
	// retry. The code is the stable contract; classify on it.
	jsErrInsufficientResources = jetstream.ErrorCode(10023) // JSInsufficientResourcesErr — transient capacity
	jsErrMemberChangeInFlight  = jetstream.ErrorCode(10202) // JSClusterServerMemberChangeInflightErr — a grow/shrink is running
	jsErrInsufficientStorage   = jetstream.ErrorCode(10047) // JSStorageResourcesExceededErr — PERMANENT (disk), not a stall
	jsErrNotEnabled            = jetstream.ErrorCode(10076) // JSNotEnabledErr
	jsErrNotEnabledForAcc      = jetstream.ErrorCode(10039) // JSNotEnabledForAccountErr
)

// IsTransientProvisionErr reports whether a JetStream provisioning error is worth retrying inside a
// REQUEST-SCOPED, BOUNDED retry (see broker.provisionXferBucket).
//
// It is deliberately SEPARATE from IsMetaGroupNotReady, and IsMetaGroupNotReady is deliberately left
// byte-unchanged. They answer different questions for callers with opposite failure modes:
//
//   - IsMetaGroupNotReady feeds the RECONCILE loop, where callers write
//     `err != nil && !errors.Is(err, ErrMetaGroupNotReady)` (audit_publisher.go:141/481/585) — a match
//     makes the error VANISH and the loop retries forever behind Degraded(). Broadening it would spin a
//     permanent misconfiguration silently for days (cf. the racknerd incident: a stale clustered
//     nats.conf 503-ing unnoticed for five days).
//   - This one feeds a bounded retry that GIVES UP and reports to a waiting human, so classifying a
//     stall as transient costs at most a few seconds.
//
// Permanent rules are evaluated FIRST and the default is PERMANENT: an unrecognised error must never
// buy retries. In particular 10047 (insufficient storage) is checked before the "insufficient"
// substring, exactly as IsMetaGroupNotReady already does.
func IsTransientProvisionErr(err error) bool {
	if err == nil {
		return false
	}

	// ── permanent: structural ────────────────────────────────────────────────────────────────
	if errors.Is(err, jetstream.ErrJetStreamNotEnabled) || errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount) {
		return false
	}
	if apiErr := asAPIErr(err); apiErr != nil {
		switch apiErr.ErrorCode {
		case jsErrNotEnabled, jsErrNotEnabledForAcc, jsErrInsufficientStorage:
			return false
		}
	}
	// ── permanent: substring (mirrors IsMetaGroupNotReady's permanent set) ───────────────────
	msg := strings.ToLower(err.Error())
	for _, perm := range []string{"non-clustered", "not supported", "insufficient storage"} {
		if strings.Contains(msg, perm) {
			return false
		}
	}
	// A cancelled parent is a shutdown, not a stall: stop immediately.
	if errors.Is(err, context.Canceled) {
		return false
	}

	// ── transient ────────────────────────────────────────────────────────────────────────────
	// A deadline expiring is the SILENT-DROP regime: nats-server returns without a reply at all
	// when it holds no JetStream leadership, so the client learns nothing until its own deadline.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// No responder = the JS API subject has no subscriber right now (server still coming up).
	if errors.Is(err, nats.ErrNoResponders) {
		return true
	}
	if apiErr := asAPIErr(err); apiErr != nil {
		switch apiErr.ErrorCode {
		case jsErrClusterNotAvail, jsErrClusterPeerNotMember, jsErrClusterNoPeers,
			jsErrClusterIncomplete, jsErrMemberChangeInFlight, jsErrInsufficientResources:
			return true
		}
	}
	// Reachable only BECAUSE we retry: a create that committed server-side but lost its reply
	// returns "stream name already in use" on the next attempt, which diverts into the
	// raise-replicas path, whose lookup can 404 while the stream assignment is still propagating.
	if errors.Is(err, jetstream.ErrStreamNotFound) || errors.Is(err, jetstream.ErrBucketNotFound) {
		return true
	}
	// Finally defer to the reconcile-loop classifier's TRANSIENT half (it is a strict subset of
	// what we accept; calling it here does not widen it for its own callers).
	if errors.Is(err, ErrMetaGroupNotReady) || IsMetaGroupNotReady(err) {
		return true
	}

	// ── default: PERMANENT ───────────────────────────────────────────────────────────────────
	return false
}

// asAPIErr unwraps to a *jetstream.APIError, which is what carries the server's ErrorCode.
func asAPIErr(err error) *jetstream.APIError {
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return nil
}
