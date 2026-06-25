package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
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
