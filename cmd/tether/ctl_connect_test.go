package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/nats-io/nats.go"
)

// ctl_connect_test.go — the ctl's connection-level bounded retry
// (connectWithAuthRetry compatibility adapter; ctl_connect.go: transientConnectErr,
// connectRetryEligible, ctlNameOf). Moved here from exec_exit_class_test.go
// by the S1 internal review (E12: tests live beside the unit they test, §3
// step 5b); the eligibility table is new (C3 / E11).

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// origin: docs/reviews/hot-update-reliable-exec-plan.md §1.5 — the bounded
// connect retry only ever spends budget for an eligible identity on a
// transient failure; a fresh identity and a non-transient error return the
// first error at once; ctx cancellation interrupts the wait.
func TestConnectWithAuthRetryPolicy(t *testing.T) {
	ioTimeout := &net.OpError{Op: "read", Net: "tcp", Err: timeoutErr{}}
	t.Run("ineligible_single_attempt", func(t *testing.T) {
		attempts, reports := 0, 0
		err := connectWithAuthRetry(context.Background(), false,
			func() (*nats.Conn, error) { attempts++; return nil, ioTimeout },
			func(int, error, time.Duration) { reports++ })
		if !errors.Is(err, ioTimeout) || attempts != 1 || reports != 0 {
			t.Fatalf("fresh identity retried: attempts=%d reports=%d err=%v", attempts, reports, err)
		}
	})
	t.Run("eligible_transient_then_success", func(t *testing.T) {
		attempts, reports := 0, 0
		start := time.Now()
		err := connectWithAuthRetry(context.Background(), true,
			func() (*nats.Conn, error) {
				attempts++
				if attempts < 3 {
					return nil, ioTimeout
				}
				return nil, nil
			},
			func(int, error, time.Duration) { reports++ })
		elapsed := time.Since(start)
		if err != nil || attempts != 3 || reports != 2 || elapsed < 700*time.Millisecond || elapsed > ctlAuthRetryBudget+time.Second {
			t.Fatalf("retry shape: attempts=%d reports=%d elapsed=%s err=%v", attempts, reports, elapsed, err)
		}
	})
	t.Run("eligible_non_transient", func(t *testing.T) {
		attempts := 0
		err := connectWithAuthRetry(context.Background(), true,
			func() (*nats.Conn, error) { attempts++; return nil, nats.ErrNkeysNotSupported }, nil)
		if !errors.Is(err, nats.ErrNkeysNotSupported) || attempts != 1 {
			t.Fatalf("non-transient error retried: attempts=%d err=%v", attempts, err)
		}
	})
	t.Run("budget_bounds_attempts", func(t *testing.T) {
		attempts := 0
		start := time.Now()
		err := connectWithAuthRetry(context.Background(), true,
			func() (*nats.Conn, error) { attempts++; return nil, nats.ErrNoServers }, nil)
		if !errors.Is(err, nats.ErrNoServers) || attempts < 3 || time.Since(start) > ctlAuthRetryBudget+time.Second {
			t.Fatalf("budget: attempts=%d elapsed=%s err=%v", attempts, time.Since(start), err)
		}
	})
	t.Run("ctx_interrupts_wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(50 * time.Millisecond); cancel() }()
		start := time.Now()
		err := connectWithAuthRetry(ctx, true, func() (*nats.Conn, error) { return nil, ioTimeout }, nil)
		if !errors.Is(err, ioTimeout) || time.Since(start) > 200*time.Millisecond {
			t.Fatalf("cancellation did not interrupt: elapsed=%s err=%v", time.Since(start), err)
		}
	})
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nats.ErrNoServers, true}, {nats.ErrTimeout, true}, {ioTimeout, true}, {nats.ErrAuthorization, true},
		{fmt.Errorf("nats: Authorization Violation"), true}, {errors.New("x"), false}, {nats.ErrNkeysNotSupported, false}, {nil, false},
	} {
		if got := transientConnectErr(tc.err); got != tc.want {
			t.Errorf("transientConnectErr(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// origin: hot-update S1 internal review C3 / E11 — the eligibility predicate
// as a table. The login arms are the negative control plan §1.5 and
// architecture §22.4 item 10 promised ("a wrong PIN fails fast"); the first
// cut compared the verb to the literal "login" and let the activation verb
// "login: activation for session <sid>" retry a wrong --pin for the whole
// budget, each retry a real Argon2 refusal charged to the per-IP PIN budget.
func TestConnectRetryEligibility(t *testing.T) {
	home := t.TempDir()
	const sid, fp, otherFP = "lab", "SHA256:this-key", "SHA256:other-key"
	if err := cli.WriteAuthReceipt(home, sid, fp); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		verb      string
		activated bool
		fp        string
		want      bool
	}{
		{"exec with receipt", "exec", true, fp, true},
		{"history with receipt", "history", true, fp, true},
		{"plain login never retries", "login", true, fp, false},
		{"login activation verb never retries", "login: activation for session " + sid, true, fp, false},
		{"login with a space verb never retries", "login --pin", true, fp, false},
		{"unactivated name (pure auth) never retries", "exec", false, fp, false},
		{"another key's receipt does not make this key eligible", "exec", true, otherFP, false},
		{"no receipt at all", "exec", true, "SHA256:fresh", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := connectRetryEligible(tc.verb, home, sid, tc.activated, tc.fp); got != tc.want {
				t.Fatalf("connectRetryEligible(%q, activated=%v, fp=%q) = %v, want %v", tc.verb, tc.activated, tc.fp, got, tc.want)
			}
		})
	}
	// A receipt for another session on the same home does not carry over.
	if connectRetryEligible("exec", home, "other-session", true, fp) {
		t.Fatal("a receipt for lab made other-session eligible")
	}
	// ctlNameOf reads the name an option list sets (the receipt is keyed by
	// the session that name carries); nothing set → "".
	if name := ctlNameOf([]nats.Option{nats.Name("tether-cli:lab")}); name != "tether-cli:lab" {
		t.Fatalf("ctlNameOf = %q", name)
	}
	if name := ctlNameOf(nil); name != "" {
		t.Fatalf("ctlNameOf(nil) = %q", name)
	}
	if sid, ok := cli.SessionFromCtlName("tether-cli:lab"); sid != "lab" || !ok {
		t.Fatalf("SessionFromCtlName = %q, %v", sid, ok)
	}
	if _, ok := cli.SessionFromCtlName("tether-cli"); ok {
		t.Fatal("an unactivated name carries no session")
	}
}

// This compatibility adapter is only used by the older retry tests; production
// callers pass the RunE deadline directly to connectWithAuthRetryWithin.
// origin: hot-update S6 plan §8.2 / T33
// connectWithAuthRetry runs attempt until it succeeds, the error is not
// transient, the caller is not eligible, the budget would be exceeded by the
// next backoff, or ctx ends. report is called before each wait.
func connectWithAuthRetry(ctx context.Context, eligible bool, attempt func() (*nats.Conn, error),
	report func(attempt int, err error, next time.Duration)) error {
	now, err := controlbudget.Now()
	if err != nil {
		return err
	}
	_, err = connectWithAuthRetryWithin(ctx, controlbudget.Deadline(now+int64(ctlAuthRetryBudget)), "compat-connect", nil, eligible,
		func(controlbudget.Deadline) (*nats.Conn, error) { return attempt() }, report)
	return err
}
