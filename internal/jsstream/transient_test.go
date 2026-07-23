package jsstream

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func apiErr(code int, errCode uint16, desc string) error {
	return &jetstream.APIError{Code: code, ErrorCode: jetstream.ErrorCode(errCode), Description: desc}
}

// TestIsTransientProvisionErr pins the #67 bounded-retry classifier. Every row names the mutation it
// kills, because a classifier whose default is "permanent" passes most naive tests with its entire
// transient half deleted.
func TestIsTransientProvisionErr(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		want  bool
		kills string
	}{
		{"nil is never transient", nil, false, "the nil guard"},

		// ── transient ──────────────────────────────────────────────────────────────────────
		{"deadline exceeded (the SILENT-DROP regime: no reply at all until our own deadline)",
			context.DeadlineExceeded, true, "the DeadlineExceeded rule — the signature drill 42 rep3 and drill 67 actually hit"},
		{"wrapped deadline exceeded still transient (errors.Is, not ==)",
			fmt.Errorf("create_bucket: %w", context.DeadlineExceeded), true, "unwrapping; a == comparison would miss every real call site"},
		{"no responders (JS API subject has no subscriber yet)",
			nats.ErrNoResponders, true, "the ErrNoResponders rule"},
		{"10008 cluster not available (meta has no quorum)",
			apiErr(503, 10008, "JetStream system temporarily unavailable"), true, "the 10008 rule"},
		{"10202 member change in progress — THE headline #67 window (a grow is running)",
			apiErr(400, 10202, "cluster member change is in progress"), true,
			"the 10202 rule; note it is HTTP-class 400, so a blanket `Code==400 => permanent` shortcut would make the whole fix a no-op for its own severity case"},
		{"10005 no suitable peers (also 400, also transient)",
			apiErr(400, 10005, "no suitable peers for placement"), true, "the 10005 rule (the second 400-but-transient code)"},
		{"10004 cluster incomplete", apiErr(503, 10004, "incomplete results"), true, "the 10004 rule"},
		{"10040 peer not a member yet", apiErr(400, 10040, "peer not a member"), true, "the 10040 rule"},
		{"stream not found — reachable ONLY because we retry (lost-reply create -> raise path -> 404 while the assignment propagates)",
			fmt.Errorf("xfer reconcile lookup OBJ_xfer-s: %w", jetstream.ErrStreamNotFound), true,
			"the propagation-race rule the retry itself makes reachable"},
		{"meta-group-not-ready sentinel still transient here",
			ErrMetaGroupNotReady, true, "the deferral to the reconcile classifier's transient half"},
		{"no suitable peer (substring path, via IsMetaGroupNotReady)",
			errors.New("no suitable peer for placement"), true, "the IsMetaGroupNotReady deferral"},

		// ── permanent ──────────────────────────────────────────────────────────────────────
		{"10047 insufficient STORAGE is permanent — a full disk is not a stall",
			apiErr(500, 10047, "insufficient storage resources available"), false,
			"the ordering guarantee: 10047 must be classified BEFORE the transient `insufficient` substring, or a full disk retries forever"},
		{"insufficient storage by substring is permanent",
			errors.New("insufficient storage resources available"), false, "the permanent substring set"},
		{"replicas>1 in non-clustered mode is permanent",
			errors.New("replicas > 1 not supported in non-clustered mode"), false, "the non-clustered permanent rule"},
		{"JetStream not enabled is permanent",
			jetstream.ErrJetStreamNotEnabled, false, "the not-enabled structural rule"},
		{"JetStream not enabled for account is permanent",
			jetstream.ErrJetStreamNotEnabledForAccount, false, "the not-enabled-for-account structural rule"},
		{"10076 not enabled (API-error form) is permanent",
			apiErr(503, 10076, "jetstream not enabled"), false, "the 10076 rule"},
		{"context.Canceled is a SHUTDOWN, not a stall — stop, do not retry",
			context.Canceled, false, "the Canceled rule; retrying through broker shutdown would delay every in-flight request"},
		{"an unrecognised error defaults to PERMANENT (never buy retries on a mystery)",
			errors.New("some brand new server error nobody has classified"), false,
			"the fail-permanent default — the single most important property of this classifier"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsTransientProvisionErr(tc.err); got != tc.want {
				t.Fatalf("IsTransientProvisionErr(%v) = %v, want %v — this row exists to kill: %s",
					tc.err, got, tc.want, tc.kills)
			}
		})
	}
}

// TestIsMetaGroupNotReadyDeliberatelyNotWidened is a two-sided pin on the UNCHANGED reconcile-loop
// classifier. #67's fix adds a SEPARATE classifier rather than widening this one, and that decision
// has to be visible in code, not only in a plan document: this classifier's callers write
// `err != nil && !errors.Is(err, ErrMetaGroupNotReady)` (audit_publisher.go:141/481/585), i.e. a match
// makes the error VANISH and the loop retries forever behind Degraded(). If someone later "fixes"
// #67 again by teaching this function about deadlines or 503s, a permanently broken cluster would go
// silent for days instead of surfacing — exactly the racknerd incident shape.
func TestIsMetaGroupNotReadyDeliberatelyNotWidened(t *testing.T) {
	mustNotMatch := []struct {
		name string
		err  error
	}{
		{"context deadline exceeded", context.DeadlineExceeded},
		{"wrapped deadline exceeded", fmt.Errorf("create_bucket: %w", context.DeadlineExceeded)},
		{"503 JetStream system temporarily unavailable", apiErr(503, 10008, "JetStream system temporarily unavailable")},
		{"cluster member change in progress", apiErr(400, 10202, "cluster member change is in progress")},
		{"no responders", nats.ErrNoResponders},
	}
	for _, tc := range mustNotMatch {
		t.Run("stays-out/"+tc.name, func(t *testing.T) {
			if IsMetaGroupNotReady(tc.err) {
				t.Fatalf("IsMetaGroupNotReady(%v) = true — this classifier feeds a FOREVER loop whose "+
					"callers make a match disappear behind Degraded(). #67 is fixed by the separate, "+
					"bounded IsTransientProvisionErr; widening THIS one turns a permanent failure into "+
					"days of silence", tc.err)
			}
			// The same input must be transient for the bounded retry — that is the whole point of
			// having two classifiers, and it fails if someone deletes IsTransientProvisionErr's rules.
			if !IsTransientProvisionErr(tc.err) {
				t.Fatalf("IsTransientProvisionErr(%v) = false; the bounded retry must accept what the "+
					"forever loop must not", tc.err)
			}
		})
	}

	// The other side: what it DID match must keep matching, so "not widened" cannot be achieved by
	// accidentally narrowing it either.
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"no suitable peer stays transient", errors.New("no suitable peer"), true},
		{"insufficient resources stays transient", errors.New("insufficient resources available"), true},
		{"insufficient STORAGE stays permanent", errors.New("insufficient storage resources available"), false},
		{"non-clustered stays permanent", errors.New("replicas > 1 not supported in non-clustered mode"), false},
	} {
		t.Run("unchanged/"+tc.name, func(t *testing.T) {
			if got := IsMetaGroupNotReady(tc.err); got != tc.want {
				t.Fatalf("IsMetaGroupNotReady(%v) = %v, want %v — this function must be byte-unchanged by #67",
					tc.err, got, tc.want)
			}
		})
	}
}
