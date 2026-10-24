package infraattach

import (
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/inframaint"
)

// origin: hot-update S9 plan §4.6 / P23 / M25
func TestClientMoveBindsActualSocketAndExpiresOriginalHold(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	p := prepareProof(t, f)
	if _, err := f.client.Install(p, attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if !f.client.Protects() || !f.client.Pending() {
		t.Fatal("installed hold did not retain its responsibility")
	}
	nc := f.nc
	moved, err := f.client.Attach(p.Target, attachDeadline(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if f.nc != nc || moved.CID == p.Old.CID || moved.Raw.Local == p.Old.Raw.Local || moved.Generation != p.Old.Generation+1 {
		t.Fatal("planned move replaced invocation or reused old socket")
	}
	p.Purpose, p.New = c.RouteBind, moved
	p, err = inframaint.SignProof(p, c.ID{4}, func(data []byte) ([]byte, error) { return ed25519.Sign(f.private, data), nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.client.Bind(p, attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	f.now.Add(int64(8 * time.Second))
	if f.client.Protects() {
		t.Fatal("expired hold still protects the route")
	}
	if _, err = f.client.Install(p, attachDeadline(t)); err == nil {
		t.Fatal("wrong purpose or expired hold was reinstalled")
	}
	if !f.client.Pending() {
		t.Fatal("expiry erased retained image exclusion")
	}
	refreshFixtureRegistration(t, f, p.Old.Generation)
}

// Installed intent cannot replay the old server pool before Attach records the
// allowed successor. This gate executes before DNS/TCP and SDK SUB replay.
// origin: hot-update S9 plan §4.6 / P23 / M25
func TestClientPrepareRefusesAutomaticOldPoolDial(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	p := prepareProof(t, f)
	if _, err := f.client.Install(p, attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	gate := &gatedDialer{client: f.client}
	raw, err := gate.DialWithin("tcp", f.server.Addr().String(), attachDeadline(t), nil)
	if raw != nil {
		_ = raw.Close()
	}
	if !errors.Is(err, c.ErrIncomplete) || raw != nil {
		t.Fatal("Prepare permitted an automatic old-pool business dial")
	}
}

// Retry preserves the receiver's original nonce/t0, and return becomes legal
// only after the outbound physical Bind. A new purpose is not a clock reset.
// origin: hot-update S9 plan §4.6 / P23 / M25
func TestClientReturnRequiresBoundOutboundAndKeepsChallengeClock(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	p := prepareProof(t, f)
	first := f.client.challenge
	f.now.Add(int64(time.Second))
	retried, err := f.client.Challenge(p.Tx, c.RouteOutbound)
	if err != nil || retried != first {
		t.Fatal("challenge retry reset the original deadline or nonce")
	}
	if _, err = f.client.Install(p, attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err = f.client.Challenge(p.Tx, c.RouteReturn); err == nil {
		t.Fatal("return bypassed outbound Bind")
	}
	moved, err := f.client.Attach(p.Target, attachDeadline(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	p.Purpose, p.New = c.RouteBind, moved
	p, err = inframaint.SignProof(p, c.ID{4}, func(data []byte) ([]byte, error) { return ed25519.Sign(f.private, data), nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.client.Bind(p, attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	refreshFixtureRegistration(t, f, p.Old.Generation)
	challenge, err := f.client.Challenge(p.Tx, c.RouteReturn)
	if err != nil {
		t.Fatal(err)
	}
	p.Purpose, p.Direction, p.Old, p.New, p.Nonce = c.RoutePrepare, c.RouteReturn, moved, c.Attachment{}, challenge.Nonce
	p, err = inframaint.SignProof(p, c.ID{4}, func(data []byte) ([]byte, error) { return ed25519.Sign(f.private, data), nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.client.Install(p, attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	returned, err := f.client.Attach(p.Target, attachDeadline(t), nil)
	if err != nil || returned.Generation != moved.Generation+1 || returned.CID == moved.CID {
		t.Fatalf("return failed to acquire its own actual attachment: %v", err)
	}
	p.Purpose, p.New = c.RouteBind, returned
	p, err = inframaint.SignProof(p, c.ID{4}, func(data []byte) ([]byte, error) { return ed25519.Sign(f.private, data), nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.client.Bind(p, attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	refreshFixtureRegistration(t, f, moved.Generation)
}
