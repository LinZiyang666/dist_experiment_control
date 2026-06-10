package main

import (
	"io"
	"strings"
	"testing"
)

// `tether proxy on` without --yes in a non-interactive context must ABORT
// (and therefore never send a NATS request that would flip the switch).
func TestProxyOnAbortsWithoutYesNonInteractive(t *testing.T) {
	root := newProxyCmd()
	root.SetArgs([]string{"on"})
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	err := root.Execute()
	if err == nil {
		t.Fatal("proxy on without --yes (non-interactive) must error, not proceed")
	}
	if !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("expected an abort error, got: %v", err)
	}
}

// `proxy sub create` requires --name.
func TestProxySubCreateRequiresName(t *testing.T) {
	root := newProxyCmd()
	root.SetArgs([]string{"sub", "create"})
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	if err := root.Execute(); err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("expected a --name-required error, got: %v", err)
	}
}

// Every P13 broker code maps to a human hint (no raw-code fallthrough).
func TestProxyErrorHintsMapped(t *testing.T) {
	for _, code := range []string{
		"not_owner", "sub_name_invalid", "sub_name_taken", "sub_not_found",
		"already_revoked", "subject_malformed", "proxy_disabled", "name_reserved",
	} {
		if _, ok := brokerCodeHints[code]; !ok {
			t.Errorf("proxy code %q has no operator hint", code)
		}
		msg := brokerErrorMessage("proxy x", code, "raw")
		if msg == nil || !strings.Contains(msg.Error(), brokerCodeHints[code]) {
			t.Errorf("code %q: error message does not include its hint", code)
		}
	}
}
