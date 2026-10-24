package xferclient

import (
	"errors"
	"fmt"
	"time"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// Error preserves the original invocation for the CLI's read-only status hint.
// Unresolved describes responsibility, not a synthetic failed terminal.
// origin: hot-update S7 plan §7 / T33
type Error struct {
	Code       string
	TransferID string
	Phase      x.Phase
	Unresolved bool
	Cause      error
}

func (e *Error) Error() string {
	message := fmt.Sprintf("%s: transfer_id=%s phase=%s", e.Code, e.TransferID, e.Phase)
	if e.Unresolved {
		message += " outcome unresolved; query transfer status"
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e *Error) Unwrap() error { return e.Cause }

// ErrorCode keeps classification independent of a peer's prose error text.
func ErrorCode(err error) string {
	var transferErr *Error
	if errors.As(err, &transferErr) {
		return transferErr.Code
	}
	return ""
}

// ExitCode is shared by the main transfer command and the read-only query.
// A state=unknown query has no error; uncertainty in Run is an explicit error.
// origin: hot-update S7 plan §7 / lead LQ1–LQ3
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	var transferErr *Error
	if !errors.As(err, &transferErr) {
		return 74
	}
	switch transferErr.Code {
	case x.CodeNotMember, x.CodeNotOwner, "not_owner", "not_allowed", "path_outside_roots":
		return 77
	case x.CodeResultUnknown, x.CodeTerminalConflict, x.CodeCommitInflight,
		x.CodeDurabilityUnknown, x.CodeReconcileRequired:
		return 74
	case x.CodeOwnerRecovering, x.CodeSessionDeleting, x.CodeStorageHorizon,
		x.CodeCapacityExceeded, "transfer_id_in_flight", "try_again", "forward_failed",
		"broker_forward_failed", "too_many_in_flight", "agent_no_responders",
		"leader_unavailable", "home_catching_up", "jetstream_not_ready",
		"broker_restarting", "cluster_not_ready", "path_race", "agent_upgrading":
		return 75
	case x.CodeIntentConflict, x.CodePermitExpired, x.CodeObjectConflict,
		x.CodeObjectVersionMismatch, x.CodeSourceChanged, x.CodePayloadUnsupported,
		x.CodeUnknown, "transfer_disabled", "path_not_absolute", "path_not_found",
		"path_parent_missing", "not_a_regular_file", "dst_exists", "too_large",
		"tier_invalid", "size_mismatch", "sha_mismatch", "bucket_unknown",
		"tier_b_store_too_small", "jetstream_unavailable", "node_offline",
		"node_not_found", "session_not_found", "verb_mismatch", "request_invalid":
		return 64
	case x.CodeBudgetExceeded:
		if transferErr.Unresolved {
			return 74
		}
		// No invocation phase exists until admission. An impossible sealed rate
		// budget is a deterministic input error before any permit or byte leaves.
		// origin: S7 internal review rv-n1compat F6 / hot-update S7 plan LQ2
		if transferErr.Phase == "" {
			return 64
		}
		return 75
	case x.CodeCanceled:
		if transferErr.Unresolved {
			return 74
		}
		return 64
	case "actor_invalid", "subject_malformed", "marshal", "json_parse",
		"agent_malformed_resp", "store_error", "internal_error", "reply_too_large",
		"io_error", "object_get_failed", "object_put_failed", "bucket_create_failed", "bucket_bind_failed":
		return 70
	default:
		// Unrecognized peer refusals cannot produce exit 0 or a guessed success.
		return 74
	}
}

func clientError(code string, identity x.Identity, phase x.Phase, unresolved bool, cause error) error {
	return &Error{Code: code, TransferID: identity.TransferID, Phase: phase, Unresolved: unresolved, Cause: cause}
}

// The diagnostic has no upload authority. Keep its error-interface methods
// outside the one-shot entrypoint so general error reporting cannot acquire
// a graph edge to that entrypoint.
// origin: hot-update S7 plan §7 / T49
// jetStreamStallKind names WHICH of the watchdog's two questions went unanswered.
type jetStreamStallKind string

const (
	// stallNoAnswer: `strikes` consecutive probes failed — the stream's API does not answer, or answers
	// with no leader. JetStream is down for this bucket; a retry now would only spend another window.
	stallNoAnswer jetStreamStallKind = "no_answer"
	// stallNoProgress: the probes answer and name a leader, yet the stream's last sequence has not moved
	// for `strikes` consecutive probes while our chunks are in flight. The upload is not landing. That is
	// what a Put looks like whose burst of chunk publishes arrived during a leader election: nats-server
	// drops publishes to a leaderless stream without a NAK, nats.go's async publisher has nothing to retry
	// on, and once the leader is elected the stream is perfectly healthy — for everybody except this Put,
	// which waits for acks that will never come until its budget expires (drill 67 CONTROL(after) attempt
	// 1 on image #8: 92 chunks / 25 MB in during the first 5 s while `leader==""`, `msgs` static for the
	// next 115 s, next attempt succeeded in 194 ms). The stream is healthy NOW, so this kind is retryable.
	stallNoProgress jetStreamStallKind = "no_progress"
)

// jetStreamStallError is the watchdog's verdict: the Put was cancelled because, for `strikes` consecutive
// probes, JetStream either stopped answering (stallNoAnswer) or accepted nothing (stallNoProgress).
type jetStreamStallError struct {
	kind    jetStreamStallKind
	strikes int
	window  time.Duration
	last    error  // stallNoAnswer: the last probe error
	lastSeq uint64 // stallNoProgress: the sequence the stream sat on
}

func (e *jetStreamStallError) Error() string {
	if e.kind == stallNoProgress {
		return fmt.Sprintf("the stream accepted none of the upload for %s (%d probes; leader present, last_seq stuck at %d)",
			e.window.Round(time.Second), e.strikes, e.lastSeq)
	}
	return fmt.Sprintf("%d probes over %s failed, last: %v", e.strikes, e.window.Round(time.Second), e.last)
}

func (e *jetStreamStallError) Unwrap() error { return e.last }
