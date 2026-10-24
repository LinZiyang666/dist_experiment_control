package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// TestClassifyExit (B2 item 3): the central classifier maps terminal errors to the sysexits-style
// taxonomy. Explicit *ExitError class wins (even when wrapped); sentinels map deterministically;
// an unclassified error defaults to 70 (NOT the old exit-1 that collided with DEGRADED).
func TestClassifyExit(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"nil", nil, exitOK},
		{"explicit usage", &ExitError{Class: exitUsage, Err: errors.New("x")}, exitUsage},
		{"explicit unavailable", unavailErr("x"), exitUnavailable},
		{"explicit perm", permErr("x"), exitNoPerm},
		{"wrapped exiterror", fmt.Errorf("ctx: %w", &ExitError{Class: exitUnavailable, Err: errors.New("y")}), exitUnavailable},
		{"no responders", fmt.Errorf("call: %w", nats.ErrNoResponders), exitUnavailable},
		{"deadline", fmt.Errorf("x: %w", context.DeadlineExceeded), exitTransient},
		{"canceled", fmt.Errorf("x: %w", context.Canceled), exitTransient},
		{"nats timeout", fmt.Errorf("x: %w", nats.ErrTimeout), exitTransient},
		{"unclassified default 70", errors.New("random boom"), exitInternal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := classifyExit(c.err); got != c.want {
				t.Errorf("classifyExit(%v) = %d, want %d", c.err, got, c.want)
			}
		})
	}
}

// TestClassifyExitNeverHealthCodes (B2 negative reservation): a non-nil error must NEVER classify
// to a cluster-status health code (0..3) — those are reserved and emitted only by `cluster status`.
func TestClassifyExitNeverHealthCodes(t *testing.T) {
	for _, err := range []error{
		errors.New("x"), unavailErr("y"), permErr("z"), usageErr("u"),
		fmt.Errorf("%w", nats.ErrNoResponders), fmt.Errorf("%w", context.DeadlineExceeded),
	} {
		got := classifyExit(err)
		if got >= 1 && got <= 3 {
			t.Errorf("classifyExit returned a reserved health code %d for %v", got, err)
		}
		if got == 0 {
			t.Errorf("classifyExit(non-nil) must not be 0 (success), got 0 for %v", err)
		}
	}
}

// TestBrokerCodeExitClassesNeverReserved: every mapped class is a real taxonomy value (never 0..3).
func TestBrokerCodeExitClassesNeverReserved(t *testing.T) {
	allowed := map[int]bool{exitUsage: true, exitUnavailable: true, exitInternal: true, exitTransient: true, exitNoPerm: true}
	for code, class := range brokerCodeExitClasses {
		if !allowed[class] {
			t.Errorf("brokerCodeExitClasses[%q] = %d is not a valid taxonomy class", code, class)
		}
	}
}

// TestClusterAdminErrorExitClass (B2 item 4): clusterAdminError classifies by resp.Code and keeps
// the "cluster <verb>: <error>" message format; a legacy (un-coded) reply defaults to 70.
func TestClusterAdminErrorExitClass(t *testing.T) {
	cases := []struct {
		code string
		want int
	}{
		{adminsock.CodeNotLeader, exitNoPerm},
		{adminsock.CodeNonceUsed, exitUsage},
		{adminsock.CodeCatchUpStalled, exitTransient},
		{adminsock.CodeStoreError, exitInternal},
		{"", exitInternal}, // legacy / un-coded → default 70
	}
	for _, c := range cases {
		err := clusterAdminError("add", &adminsock.Response{Code: c.code, Error: "boom"})
		var ee *ExitError
		if !errors.As(err, &ee) {
			t.Fatalf("code %q: not an *ExitError: %v", c.code, err)
		}
		if ee.Class != c.want {
			t.Errorf("code %q: class = %d, want %d", c.code, ee.Class, c.want)
		}
		if !strings.Contains(err.Error(), "cluster add: boom") {
			t.Errorf("code %q: message format wrong: %q", c.code, err.Error())
		}
	}
}

// TestEmitJSONEncodeFailure: an unmarshalable payload yields an internal (70) error.
func TestEmitJSONEncodeFailure(t *testing.T) {
	err := emitJSON(io.Discard, make(chan int)) // channels can't marshal
	var ee *ExitError
	if !errors.As(err, &ee) || ee.Class != exitInternal {
		t.Errorf("encode failure must be *ExitError{exitInternal}, got %v", err)
	}
}

// origin: hot-update S3 plan §4.2 / T17. A generic hint must never hide why a node
// deferred, and a busy node must not stop a fleet once its canary has committed.
// Local NATS responders drive the real ctl dispatcher and fleet loop; agent emission
// and same-PID image continuity are covered by the agent and real-binary suites.
// origin: hot-update S5 plan §7.3 / §10.2. Keep the S5 reason retryable
// both directly and through the broker's agent_rejected wrapper; real upgrade
// replies still carry upgrade_deferred so N-1 clients retain their 75 class.
func TestUpgradeDeferredCodesAreClassified(t *testing.T) {
	const detail = "deferred: legacy_session_busy (1 legacy listener); ss_state_unrepresentable (SS state is not representable)"
	for _, tc := range []struct {
		code      string
		class     int
		transient bool
		config    bool
	}{
		{"upgrade_deferred", 75, true, false},
		{"agent_upgrading", 75, false, false},
		{"ss_state_unrepresentable", 75, false, false},
		{"image_abi_unsupported", 64, false, true},
	} {
		for _, prefix := range []string{"", "agent_rejected:"} {
			t.Run(prefix+tc.code, func(t *testing.T) {
				err := brokerErrorMessage("upgrade lab/n1", prefix+tc.code, detail)
				if got := classifyExit(err); got != tc.class {
					t.Errorf("exit class = %d, want %d: %v", got, tc.class, err)
				}
				if got := wireCodeOf(err); got != tc.code {
					t.Errorf("wire code = %q, want %q", got, tc.code)
				}
				if transientUpgradeCodes[tc.code] != tc.transient || isTransientError(err) != tc.transient {
					t.Errorf("%s: transient fleet classification must be %v", tc.code, tc.transient)
				}
				if configUpgradeCodes[tc.code] != tc.config || isConfigError(err) != tc.config {
					t.Errorf("%s: config fleet classification must be %v", tc.code, tc.config)
				}
				if tc.code == "upgrade_deferred" {
					want := fmt.Sprintf("upgrade lab/n1 failed: %s (%s)", detail, prefix+tc.code)
					if err.Error() != want {
						t.Errorf("deferral rendering = %q, want full reason text %q", err, want)
					}
					if _, exists := brokerCodeHints[prefix+tc.code]; exists {
						t.Error("upgrade_deferred must have no registered hint")
					}
				}
			})
		}
	}
	for _, reason := range []string{"agent_upgrading", "agent_upgrading: image switch in progress; retry"} {
		for _, render := range []func(string) error{execFailureMessage, runFailureMessage} {
			err := render(reason)
			if classifyExit(err) != 75 || !strings.Contains(err.Error(), "retry in a few seconds") {
				t.Errorf("admission refusal must retain class 75 and retry guidance: %v", err)
			}
		}
	}
	for _, render := range []func(string) error{execFailureMessage, runFailureMessage} {
		err := render("ss_state_unrepresentable: SS state is not representable")
		if classifyExit(err) != 75 || !strings.Contains(err.Error(), "supported state and retry") {
			t.Errorf("SS deferral must retain class 75 and retry guidance: %v", err)
		}
	}

	// origin: hot-update S3 plan §7 T17. A future ABI is refused even on an empty
	// node, so neither losing the version pair nor unconditionally advising a drain is safe.
	for _, tc := range []struct{ name, detail string }{
		{"missing ABI", "candidate has no image-abi line and this node carries 1 live task(s); pick a hot-update-capable release or drain first"},
		{"future ABI", fmt.Sprintf("candidate image-abi v%d, agent supports v%d", proto.ImageABIVersion+1, proto.ImageABIVersion)},
		{"no diagnostic", ""},
	} {
		for _, prefix := range []string{"", "agent_rejected:"} {
			t.Run(prefix+tc.name, func(t *testing.T) {
				code := prefix + "image_abi_unsupported"
				err := brokerErrorMessage("upgrade lab/n1", code, tc.detail)
				if classifyExit(err) != 64 || wireCodeOf(err) != "image_abi_unsupported" || !isConfigError(err) || isTransientError(err) {
					t.Errorf("ABI refusal lost its terminal configuration class: %v", err)
				}
				hint := brokerCodeHints["image_abi_unsupported"]
				if !strings.Contains(hint, "image ABI compatibility check") ||
					!strings.Contains(hint, "If the candidate has no image-abi line, pick a hot-update-capable release or drain the node first.") {
					t.Errorf("ABI guidance must cover future versions and qualify the legacy remedy: %q", hint)
				}
				message := hint
				if tc.detail != "" {
					message = tc.detail + "; " + hint
				}
				want := fmt.Sprintf("upgrade lab/n1 failed: %s (%s)", message, code)
				if err.Error() != want {
					t.Errorf("ABI refusal rendering = %q, want diagnostic and guidance %q", err, want)
				}
			})
		}
	}

	// The responder changes the canary release before acknowledging its upgrade,
	// so confirmation needs no timer delay and never depends on a readiness sleep.
	oldPoll := upgradeWaitPoll
	upgradeWaitPoll = 0
	t.Cleanup(func() { upgradeWaitPoll = oldPoll })
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, tc := range []struct {
			name      string
			rejectNID string
			code      string
			class     int
			calls     []string
			output    []string
		}{
			{"deferred node is skipped", "n2", "upgrade_deferred", 0, []string{"n1", "n2", "n3"},
				[]string{"lab/n2 skipped (transient):", detail, "1 node(s) skipped", "COMMITTED", "fleet dispatched"}},
			{"deferred canary aborts", "n1", "upgrade_deferred", 75, []string{"n1"}, nil},
			{"incompatible candidate aborts", "n2", "image_abi_unsupported", 64, []string{"n1", "n2"}, nil},
		} {
			t.Run(fmt.Sprintf("fleet/%d/%s", round, tc.name), func(t *testing.T) {
				nc, err := nats.Connect(testharness.StartNATS(t))
				if err != nil {
					t.Fatal(err)
				}
				defer nc.Close()
				release := &atomic.Value{}
				release.Store("v0.0.1-old")
				stubNodeList(t, nc, "n1", release)
				calls := make(chan string, 3)
				for _, nid := range []string{"n1", "n2", "n3"} {
					_, err := nc.Subscribe(proto.SubjCmdBy("lab", "actor-pub", nid, "upgrade"), func(msg *nats.Msg) {
						calls <- nid
						resp := proto.UpgradeResp{OK: true, NewVersion: "v0.0.9-new"}
						switch nid {
						case tc.rejectNID:
							resp = proto.UpgradeResp{Code: "agent_rejected:" + tc.code, Error: detail}
						case "n1":
							release.Store(resp.NewVersion)
						}
						body, _ := json.Marshal(resp)
						_ = msg.Respond(body)
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := nc.Flush(); err != nil {
					t.Fatal(err)
				}
				cmd, out := waitTestCmd(t)
				err = runUpgradeAll(cmd, nc, "lab", "actor-pub", []string{"n1", "n2", "n3"}, "https://example.invalid/tether.tar.gz", "deadbeef", 5*time.Second)
				if got := classifyExit(err); got != tc.class {
					t.Errorf("fleet exit class = %d, want %d: %v; output: %s", got, tc.class, err, out.String())
				}
				if tc.class != 0 && wireCodeOf(err) != tc.code {
					t.Errorf("fleet lost refusal code %q: %v", tc.code, err)
				}
				if tc.rejectNID == "n1" && (err == nil || !strings.Contains(err.Error(), "canary n1 failed, 2 node(s) untouched")) {
					t.Errorf("deferred canary must leave the fleet untouched: %v", err)
				}
				var gotCalls []string
				for len(calls) > 0 {
					gotCalls = append(gotCalls, <-calls)
				}
				if !slices.Equal(gotCalls, tc.calls) {
					t.Errorf("dispatched nodes = %v, want %v", gotCalls, tc.calls)
				}
				for _, want := range tc.output {
					if !strings.Contains(out.String(), want) {
						t.Errorf("fleet output lost %q: %s", want, out.String())
					}
				}
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "upgrade refusal fleet", beforeG)
	// Allow only the process-wide netpoller descriptors initialized by the first NATS connection.
	testharness.AssertNoFDLeak(t, "upgrade refusal fleet", beforeFD, 2)
}
