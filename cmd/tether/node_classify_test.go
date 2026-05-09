package main

import (
	"errors"
	"testing"
)

// TestIsTransientError covers the codes `--all` skips with a
// warning rather than aborting. Reviewer's recommendation in P10
// round-1 was to keep fleet rollouts moving past one OFFLINE box;
// these are the codes architecture J.4 / G considers retryable.
func TestIsTransientError(t *testing.T) {
	transient := []string{
		"broker rejected upgrade for lab/n1: node_offline OFFLINE",
		"broker rejected upgrade for lab/n1: node_not_found ",
		"broker rejected upgrade for lab/n1: agent_no_responders nats: no responders",
		"broker rejected upgrade for lab/n1: agent_malformed_resp eof",
		"upgrade lab/n1: context deadline exceeded",
		"upgrade lab/n1: context canceled",
	}
	for _, s := range transient {
		if !isTransientError(errors.New(s)) {
			t.Errorf("transient: %q misclassified as non-transient", s)
		}
	}
	notTransient := []string{
		"broker rejected upgrade for lab/n1: not_owner ",
		"broker rejected upgrade for lab/n1: url_not_allowed https://evil/",
		"broker rejected upgrade for lab/n1: sha256_invalid foo",
	}
	for _, s := range notTransient {
		if isTransientError(errors.New(s)) {
			t.Errorf("non-transient: %q misclassified as transient", s)
		}
	}
}

// TestIsConfigError covers the codes that abort `--all` because
// the request itself is wrong; no other node will accept it
// either, so dispatching the rest is wasted broker work.
func TestIsConfigError(t *testing.T) {
	configErrs := []string{
		"broker rejected upgrade for lab/n1: not_owner ",
		"broker rejected upgrade for lab/n1: url_not_allowed https://evil/",
		"broker rejected upgrade for lab/n1: sha256_invalid bad",
		"broker rejected upgrade for lab/n1: proto_bump_requires_reinstall ",
		"broker rejected upgrade for lab/n1: actor_invalid bad",
		"broker rejected upgrade for lab/n1: session_not_found_or_deleting ",
	}
	for _, s := range configErrs {
		if !isConfigError(errors.New(s)) {
			t.Errorf("config error: %q misclassified as non-config", s)
		}
	}
	notConfig := []string{
		"broker rejected upgrade for lab/n1: node_offline OFFLINE",
		"broker rejected upgrade for lab/n1: agent_rejected:install_failed disk full",
	}
	for _, s := range notConfig {
		if isConfigError(errors.New(s)) {
			t.Errorf("non-config: %q misclassified as config", s)
		}
	}
}
