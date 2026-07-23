// Adopted from the Stage-C internal review (CLAUDE.md §3 step 5): the reviewer authored these pins,
// the main process reviewed, renamed and now owns them.
package jsstream

import (
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// ── G67 internal review, TEST-ADEQUACY lane ──────────────────────────────────────────────────────
//
// TestIsTransientProvisionErr says every row "names the mutation it kills". For the TRANSIENT rows
// that is true. For the PERMANENT rows it is not: the classifier's default is already PERMANENT, so a
// permanent row only kills something if its input would otherwise be caught by a TRANSIENT rule.
// Measured by mutation against the working tree — each of these individually keeps
// `go test ./internal/jsstream/` GREEN:
//
//	drop jsErrInsufficientStorage (10047) from the permanent APIError switch
//	drop jsErrNotEnabled / jsErrNotEnabledForAcc from the permanent APIError switch
//	drop the errors.Is(ErrJetStreamNotEnabled) rule
//	drop the context.Canceled rule
//	drop ANY ONE of the three permanent substrings ("non-clustered" / "not supported" /
//	  "insufficient storage")
//
// Most of those are harmless redundancy. ONE is not, and this file pins it.

// TestG67TLPermanent10047RuleIsLoadBearing is the row the shipped suite believes it has.
//
// TestIsTransientProvisionErr's 10047 row uses the description "insufficient storage resources
// available", which the PERMANENT SUBSTRING rule matches on its own — so the row passes with the
// 10047 code rule deleted, and it cannot see the failure it announces ("a full disk retries
// forever"). This input separates them: the server's own reason text does not always contain the
// literal "storage", and without it the classifier falls through to the IsMetaGroupNotReady
// deferral, whose TRANSIENT list contains the bare substring "insufficient" (replicas.go:116).
//
// Concretely, IsMetaGroupNotReady reports TRUE for this error (verified) — so the 10047 case in the
// permanent switch is the ONLY thing standing between a disk-full broker and buying the full retry
// budget on every single tier-B push, which is head-of-line blocking for every other transfer on the
// broker (plan G67 risk R2).
func TestG67Permanent10047RuleIsLoadBearing(t *testing.T) {
	err := &jetstream.APIError{
		Code: 500, ErrorCode: jetstream.ErrorCode(10047),
		Description: "insufficient resources available", // NOTE: no literal "storage"
	}
	if !IsMetaGroupNotReady(err) {
		t.Fatalf("premise check: this test only has bite while the reconcile classifier calls this "+
			"error transient (it matches its bare `insufficient` rule). It no longer does, so re-derive "+
			"the discriminating input before trusting this pin: %v", err)
	}
	if IsTransientProvisionErr(err) {
		t.Fatalf("10047 (JSStorageResourcesExceededErr) must be PERMANENT whatever the server's reason " +
			"text says: a full disk is not a stall, and retrying it burns the whole provisioning budget " +
			"inside a handler that serialises every push on this broker. Got transient=true, which means " +
			"the 10047 case is gone from the permanent switch and the IsMetaGroupNotReady deferral " +
			"(whose transient list contains the bare substring `insufficient`) claimed it")
	}
}

// TestG67TLTransientCodeRulesSurviveANeutralDescription pins the transient API-error codes
// STRUCTURALLY. Two of the shipped rows (10005 "no suitable peers for placement", and 10047's twin
// above) pass through the SUBSTRING half of the classifier rather than the code half, so the code
// rule they name is not actually held down — dropping jsErrClusterNoPeers keeps
// `go test ./internal/jsstream/` GREEN. Real server payloads do not always carry those words.
func TestG67TransientCodeRulesSurviveANeutralDescription(t *testing.T) {
	for _, tc := range []struct {
		name string
		code uint16
	}{
		{"10008 JSClusterNotAvailErr", 10008},
		{"10004 JSClusterIncompleteErr", 10004},
		{"10005 JSClusterNoPeersErrF (the row whose description carries it today)", 10005},
		{"10040 JSClusterPeerNotMemberErr", 10040},
		{"10202 JSClusterServerMemberChangeInflightErr — THE #67 grow window", 10202},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := &jetstream.APIError{Code: 400, ErrorCode: jetstream.ErrorCode(tc.code), Description: "request failed"}
			if !IsTransientProvisionErr(err) {
				t.Fatalf("err_code=%d must be transient on the CODE alone — a neutral reason string must "+
					"not turn the grow/election window into a terminal refusal: %v", tc.code, err)
			}
		})
	}
}

// TestG67TLBucketNotFoundIsTransient covers the half of the propagation-race rule the shipped table
// omits. Rule 8 exists because the retry itself makes the race reachable: a create that committed
// server-side but lost its reply returns "stream name already in use" next time, diverting into
// raiseXferReplicas, whose ObjectStore lookup can 404 while the assignment propagates. That lookup
// can surface as EITHER ErrStreamNotFound or ErrBucketNotFound; only the former has a row.
func TestG67BucketNotFoundIsTransient(t *testing.T) {
	if !IsTransientProvisionErr(jetstream.ErrBucketNotFound) {
		t.Fatal("ErrBucketNotFound must be transient: it is the object-store-shaped half of the " +
			"lost-reply propagation race that the bounded retry itself creates")
	}
}
