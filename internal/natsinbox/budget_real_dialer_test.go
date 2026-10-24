package natsinbox_test

import (
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proxydial"
	"github.com/nats-io/nats.go"
)

// The same multi-URL black-hole oracle as deep_dial_pending_P19, but bound to
// the landed proxydial.OptionsWithin instead of the contractstub adapter that
// still fails "not implemented (S6 P19)".
// origin: hot-update S6 internal review rv-ctl
func TestInboxMultiURLDialSharesRealBoundedDialer(t *testing.T) {
	requireInboxClock(t)
	addresses := make([]string, 3)
	for i := range addresses {
		address, closeHole := inboxBlackhole(t)
		defer closeHole()
		addresses[i] = "nats://" + address
	}
	const budget = 350 * time.Millisecond
	deadline := inboxTestDeadline(t, budget)
	observer := &inboxObserver{}
	opts, err := proxydial.OptionsWithin(proxydial.Env(func(string) (string, bool) { return "", false }), deadline, observer)
	if err != nil {
		t.Fatal(err)
	}
	opts = append(opts, inboxTestOptions()...)
	start := time.Now()
	nc, fallback, err := natsinbox.ConnectWithin(strings.Join(addresses, ","), inboxTestIdentity, opts, deadline, observer)
	err = proxydial.FinishWithin(opts, err)
	if nc != nil {
		nc.Close()
	}
	if err == nil || nc != nil || fallback || time.Since(start) > budget+120*time.Millisecond {
		t.Fatalf("multi-URL dial exceeded the shared deadline: conn=%v fallback=%v elapsed=%s err=%v", nc != nil, fallback, time.Since(start), err)
	}
	if len(observer.snapshot()) == 0 {
		t.Fatal("deep dial attempt disappeared")
	}
}

// Failover is why the ctl dials a URL list (DialFor, floor-last). One
// absolute admission budget must still leave the later URLs a share: a
// first broker host that accepts TCP but never sends INFO (hung host, or a
// black-holed SYN on a dead host) must not consume the whole budget and
// strand a healthy second URL. Options mirror cli.ConnectWithin, which
// appends nats.Timeout(remaining) after proxydial.OptionsWithin.
// origin: hot-update S6 internal review rv-ctl
func TestInboxMultiURLFailoverReachesHealthyURLWithinBudget(t *testing.T) {
	requireInboxClock(t)
	hole, closeHole := inboxBlackhole(t)
	defer closeHole()
	healthy := startInboxServer(t, true)
	defer stopInboxServer(healthy)
	const budget = 1500 * time.Millisecond
	deadline := inboxTestDeadline(t, budget)
	opts, err := proxydial.OptionsWithin(proxydial.Env(func(string) (string, bool) { return "", false }), deadline, nil)
	if err != nil {
		t.Fatal(err)
	}
	opts = append(opts, inboxTestOptions()...)
	opts = append(opts, nats.Timeout(budget))
	nc, _, err := natsinbox.ConnectWithin("nats://"+hole+","+healthy.ClientURL(), inboxTestIdentity, opts, deadline, nil)
	err = proxydial.FinishWithin(opts, err)
	if nc != nil {
		defer nc.Close()
	}
	if err != nil || nc == nil {
		t.Fatalf("a hung first URL stranded the healthy failover URL: err=%v", err)
	}
}
