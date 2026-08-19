package main

import (
	"errors"
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
		// A8: `session list` is not a real verb — it printed help and exited 0.
		{"session_not_found_or_deleting", "session ls"},
		{"node_offline", "tether agent --session"},
		{"node_not_found", "tether ps"},
		{"agent_no_responders", "agent isn't reachable"},
		{"url_not_allowed", "broker.upgrade.url_allow"},
		{"sha256_invalid", "64 lowercase hex"},
		{"sha256_mismatch", "doesn't match"},
		{"proto_bump_requires_reinstall", "full reinstall"},
		{"clone_family_upgrade_unsupported", "rebuild the source image"},
		{"name_taken", "expose rm"},
		{"port_exhausted", "free public port"},
		{"port_taken", "already allocated"},
		{"port_out_of_band", "14000-14999"},
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

// TestBrokerCodeHintsTransientCluster (B1 item 6): the three failover/transient codes each render
// a "wait and retry" sentence. All three are agent-internal today (home_catching_up / try_again are
// agent tunnel-REGISTER deny reasons, leader_unavailable is consumed by the agent register loop);
// the entries are defensive future-proofing + a log-reading gloss. None must imply user fault.
func TestBrokerCodeHintsTransientCluster(t *testing.T) {
	for _, code := range []string{"home_catching_up", "leader_unavailable", "try_again"} {
		t.Run(code, func(t *testing.T) {
			hint, ok := brokerCodeHints[code]
			if !ok || hint == "" {
				t.Fatalf("%q has no hint", code)
			}
			if !strings.Contains(hint, "transient") || !strings.Contains(hint, "retry") {
				t.Errorf("%q hint should frame it as transient/retry: %q", code, hint)
			}
		})
	}
}

// TestBrokerErrorMessageExitClass (B2 item 3): brokerErrorMessage returns an *ExitError carrying
// the code's exit class (prefix-stripped), so the process exit reflects the failure kind. The
// human string is unchanged (covered by TestBrokerErrorMessageRegisteredCodes).
func TestBrokerErrorMessageExitClass(t *testing.T) {
	cases := []struct {
		code string
		want int
	}{
		{"not_owner", exitNoPerm},
		{"port_exhausted", exitUsage},
		{"store_error", exitInternal},
		{"leader_unavailable", exitTransient},
		{"some_unmapped_future_code", exitInternal},   // default 70
		{"agent_rejected:sha256_mismatch", exitUsage}, // prefix-stripped class lookup
	}
	for _, c := range cases {
		t.Run(c.code, func(t *testing.T) {
			err := brokerErrorMessage("verb", c.code, "raw")
			var ee *ExitError
			if !errors.As(err, &ee) {
				t.Fatalf("%s: brokerErrorMessage must return an *ExitError, got %T", c.code, err)
			}
			if ee.Class != c.want {
				t.Errorf("%s: exit class = %d, want %d", c.code, ee.Class, c.want)
			}
			// raw code is still grep-able in the message
			if !strings.Contains(err.Error(), c.code) {
				t.Errorf("%s: raw code dropped from message: %q", c.code, err.Error())
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
		"attach_timeout": "subscribe in time",
		// origin: line-2 §12 Y2, corrected twice by the line-2 review.
		//
		// `/dev/ptmx` moved from pty_alloc_failed to pty_unavailable when the emitter split by errno.
		// pty_alloc_failed is the RESOURCE-EXHAUSTED case (EMFILE/ENFILE/ENOSPC/ENOMEM/EAGAIN — transient,
		// exit 75) and pty_unavailable is the host that cannot provide a PTY at all (terminal, exit
		// **64**, not 69: review M17 found 69 sits in usage.md's retryable class while this code's own
		// hint says retrying will not help, so it went to 64 alongside download_http_status and the
		// repo's other "a human must change something" codes).
		//
		// The pinned substring is `/proc/sys/kernel/pty/max`, not "file descriptors": review M17 showed
		// the hint named only the less likely exhaustion, because devpts index exhaustion returns ENOSPC
		// rather than EMFILE. Pinning the pty-limit phrase is what keeps the fix from being reworded away.
		"pty_alloc_failed":        "/proc/sys/kernel/pty/max",
		"pty_unavailable":         "/dev/ptmx",
		"attach_subscribe_failed": "attach subject",
		"exec_failed":             "command failed to start",
		"argv_required":           "no command",
		// download_http_status / download_too_large are deliberately absent: they are
		// UpgradeForwardedResp.Code values, so their hints live in brokerCodeHints and are exercised by
		// the broker-code hint test, not by runFailureMessage.
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

// TestDataplaneNotConvergedCodeIsWireStable is the pin both sides' comments promise
// and that neither side actually had.
//
// `dataplane_not_converged` is authored in internal/broker (codeDataplaneNotConverged)
// and re-declared here (dataplaneNotConvergedCode). The two packages have no
// compile-time link — cmd/tether must not import internal/broker — so a rename on
// either side compiles, tests, and lints perfectly clean while silently reclassifying
// the code from exit 75 (EX_TEMPFAIL, retry-later) to the default exit 70 (internal
// error). That downgrade would strip the meaning from R8a's headline fix: `cluster
// drain` refusing rc=0 on an unconverged data plane.
//
// The literal is asserted directly rather than compared to the broker constant,
// because importing it is exactly the dependency that does not exist.
func TestDataplaneNotConvergedCodeIsWireStable(t *testing.T) {
	const wire = "dataplane_not_converged"
	if dataplaneNotConvergedCode != wire {
		t.Fatalf("dataplaneNotConvergedCode = %q, want %q — internal/broker emits the latter "+
			"(codeDataplaneNotConverged); a mismatch silently reclassifies an unconverged drain "+
			"from exit 75 to exit 70", dataplaneNotConvergedCode, wire)
	}
	if got := brokerCodeExitClass(wire); got != exitTransient {
		t.Fatalf("brokerCodeExitClass(%q) = %d, want %d (EX_TEMPFAIL). Above all it must not be 0: "+
			"a drain that committed the control-plane write but whose data plane never followed "+
			"MUST NOT report success.", wire, got, exitTransient)
	}
	if exitTransient == 0 {
		t.Fatal("exitTransient must be nonzero")
	}
}

// TestRunFailureMessageSplitsCodeFromDetail is batch-A A1 Step 4.
//
// internal/broker/run.go builds RunChunk.Reason as `"<code>: " + err.Error()`
// in 33 places. runFailureMessage used to look up the whole string, so none of
// those matched a hint or an exit class and every one of them exited 70 — the
// class docs/usage.md §9.13 tells automation to retry with backoff. A member
// hitting `not_a_member` was told to keep retrying.
func TestRunFailureMessageSplitsCodeFromDetail(t *testing.T) {
	tests := []struct {
		reason    string
		wantClass int
		wantIn    string
	}{
		// bare code, already worked before
		{"not_a_member", exitNoPerm, "not_a_member"},
		// code + detail: the shape that used to fall through to 70
		{"not_a_member: fp 0xdead not in session", exitNoPerm, "not_a_member"},
		{"store_error: database is locked", exitInternal, "store_error"},
		{"node_offline: last heartbeat 5m ago", exitUsage, "node_offline"},
		{"actor_invalid: bad nkey", exitInternal, "actor_invalid"},
		// unknown code still classifies as internal, and keeps the raw text
		{"something_unheard_of: detail", exitInternal, "something_unheard_of"},
	}
	for _, tc := range tests {
		t.Run(tc.reason, func(t *testing.T) {
			err := runFailureMessage(tc.reason)
			var ee *ExitError
			if !errors.As(err, &ee) {
				t.Fatalf("runFailureMessage returned %T, not *ExitError — it would fall through to 70", err)
			}
			if ee.Class != tc.wantClass {
				t.Errorf("reason %q exited %d, want %d", tc.reason, ee.Class, tc.wantClass)
			}
			if !strings.Contains(err.Error(), tc.wantIn) {
				t.Errorf("message %q lost the raw code %q; logs and bug reports grep for it",
					err.Error(), tc.wantIn)
			}
			// The full reason (code AND detail) must survive into the message —
			// dropping the detail would trade one usability problem for another.
			if !strings.Contains(err.Error(), tc.reason) {
				t.Errorf("message %q dropped the detail from %q", err.Error(), tc.reason)
			}
		})
	}
}
