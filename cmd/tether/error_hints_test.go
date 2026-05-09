package main

import (
	"strings"
	"testing"
)

// TestBrokerErrorMessageRegisteredCodes pins the codes whose
// hints are intentionally exposed to the operator. A regression
// that drops one of these from brokerCodeHints would silently
// fall back to the bare code+error format — which defeats the
// point of P11/3.
func TestBrokerErrorMessageRegisteredCodes(t *testing.T) {
	cases := []struct {
		code    string
		hintHas string
	}{
		{"not_owner", "session owner"},
		{"not_a_member", "tether login"},
		{"session_not_found_or_deleting", "session list"},
		{"node_offline", "tether agent --session"},
		{"node_not_found", "tether ps"},
		{"agent_no_responders", "agent isn't reachable"},
		{"url_not_allowed", "broker.upgrade.url_allow"},
		{"sha256_invalid", "64 lowercase hex"},
		{"sha256_mismatch", "doesn't match"},
		{"proto_bump_requires_reinstall", "full reinstall"},
		{"name_taken", "expose rm"},
		{"port_exhausted", "free public port"},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			err := brokerErrorMessage("verb", tc.code, "raw")
			msg := err.Error()
			if !strings.Contains(msg, tc.hintHas) {
				t.Errorf("hint missing for %q: got %q, want substring %q",
					tc.code, msg, tc.hintHas)
			}
			if !strings.Contains(msg, tc.code) {
				t.Errorf("raw code missing for grep / log search: got %q", msg)
			}
		})
	}
}

// TestBrokerErrorMessageUnknownCodeFallsBack: an unrecognized
// code MUST still surface to the operator with the raw error
// (don't silently swallow).
func TestBrokerErrorMessageUnknownCodeFallsBack(t *testing.T) {
	err := brokerErrorMessage("verb", "future_code_we_dont_know", "details here")
	msg := err.Error()
	if !strings.Contains(msg, "details here") {
		t.Errorf("unknown code: detail dropped: %q", msg)
	}
	if !strings.Contains(msg, "future_code_we_dont_know") {
		t.Errorf("unknown code: raw code dropped: %q", msg)
	}
}

// TestBrokerErrorMessageStripsAgentRejectedPrefix: the broker
// wraps agent-side rejects as `agent_rejected:install_failed`.
// The hint table keys on the bare code, so we strip before lookup.
func TestBrokerErrorMessageStripsAgentRejectedPrefix(t *testing.T) {
	err := brokerErrorMessage("upgrade", "agent_rejected:sha256_mismatch", "got X want Y")
	if !strings.Contains(err.Error(), "doesn't match") {
		t.Errorf("agent_rejected:sha256_mismatch should pick up sha256_mismatch hint; got %q", err.Error())
	}
}

// TestConnectErrorMentionsURL: the operator hits this every time
// they typo --nats-url; the URL MUST appear so they can immediately
// see what they're connecting to.
func TestConnectErrorMentionsURL(t *testing.T) {
	url := "nats://wrong.example.com:4222"
	err := connectError("exec", url, errStub("dial tcp: connection refused"))
	if !strings.Contains(err.Error(), url) {
		t.Errorf("connect error should include URL %q; got %q", url, err.Error())
	}
	if !strings.Contains(err.Error(), "verify the broker") {
		t.Errorf("connect error should hint at broker verification; got %q", err.Error())
	}
}

// TestRunFailureMessageMapsKnownReasons covers the agent-emitted
// PTY failure reasons (architecture C.5.1). New reasons added to
// the agent should also be added to runFailureReasons; this test
// pins the current set.
func TestRunFailureMessageMapsKnownReasons(t *testing.T) {
	for reason, want := range map[string]string{
		"attach_timeout":   "subscribe in time",
		"pty_alloc_failed": "/dev/ptmx",
		"exec_failed":      "command failed to start",
		"argv_required":    "no command",
	} {
		t.Run(reason, func(t *testing.T) {
			err := runFailureMessage(reason)
			if !strings.Contains(err.Error(), want) {
				t.Errorf("reason %q: got %q, want substring %q", reason, err.Error(), want)
			}
		})
	}
	// Unknown reason still surfaces.
	err := runFailureMessage("some_future_reason")
	if !strings.Contains(err.Error(), "some_future_reason") {
		t.Errorf("unknown reason: should still surface raw value; got %q", err.Error())
	}
}

type errStub string

func (e errStub) Error() string { return string(e) }
